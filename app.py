"""
MusicDL API Server - Complete in One File
Supports: Amazon Music, Deezer, Apple Music
Features: Account rotation, CloudFlare R2 upload, auto-deletion
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pyrogram import Client, filters
from pyrogram.handlers import MessageHandler
import boto3
from botocore.config import Config
import asyncio
import os
from typing import Optional, Dict, Any
from io import BytesIO
import uvicorn
import tempfile
import threading
import time
import concurrent.futures
from asyncio import Lock, Event

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()  # Load .env file before any os.getenv() calls

# Thread pool for parallel operations
upload_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

# Pending response handlers: key = "account:bot_username" → {"event": Event, "message": None}
pending_responses: Dict[str, Dict[str, Any]] = {}

# File cache for streaming: key = file_unique_id → {"account": str, "message": Message, "expires": float}
file_cache: Dict[str, Dict[str, Any]] = {}
FILE_CACHE_TTL = 3600  # 1 hour TTL for cached file references

# Per-account locks to prevent concurrent Telegram requests on same account
# This is critical because Pyrogram clients are NOT thread-safe
account_locks: Dict[str, Lock] = {}

# ============================================================================
# CONFIGURATION
# ============================================================================

# R2 Configuration
R2_ACCOUNT_ID = os.getenv('R2_ACCOUNT_ID', "105af3e7978d3275bba8503df71a9f7c")
R2_ACCESS_KEY_ID = os.getenv('R2_ACCESS_KEY_ID', "62f5ef7ac395897bc29380c87a8a1492")
R2_SECRET_ACCESS_KEY = os.getenv('R2_SECRET_ACCESS_KEY', "14ceeef6f9f297071450c2ac8e021745d59867d838cb78879ad4d9df63d1847d")
R2_BUCKET_NAME = os.getenv('R2_BUCKET_NAME', "musicdl-apidownloader")
R2_PUBLIC_URL = "https://pub-c0ae3b9ba00e4274895218086ac91fd7.r2.dev"

# Music bot usernames
MUSIC_BOTS = {
    'amazon': "GlomaticoBlueMusicBot",
    'deezer': "deezload2bot",
    'apple': "GlomaticoPinkMusicBot"
}

# Account rotation strategy
ROTATION_STRATEGY = os.getenv('ROTATION_STRATEGY', 'round_robin')

# Session directory
SESSIONS_DIR = "telegram_sessions"
os.makedirs(SESSIONS_DIR, exist_ok=True)

# ============================================================================
# PRE-INITIALIZED S3 CLIENT (speeds up uploads by ~500ms per request)
# ============================================================================

s3_client = None

def get_s3_client():
    """Get pre-initialized S3 client (created once, reused)."""
    global s3_client
    if s3_client is None:
        s3_client = boto3.client(
            's3',
            endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            config=Config(
                signature_version='s3v4',
                max_pool_connections=50,  # Connection pooling
                connect_timeout=5,
                read_timeout=60,
                retries={'max_attempts': 3}
            ),
            region_name='auto'
        )
    return s3_client

# ============================================================================
# TELEGRAM ACCOUNT MANAGEMENT
# ============================================================================

# Global clients dictionary and rotation index
telegram_clients: Dict[str, Client] = {}
current_account_index = 0

# Request queue tracking (prevent concurrent requests on same account/bot)
pending_requests: Dict[str, int] = {}  # account -> count of pending requests

# ============================================================================
# QUEUE TRACKING SYSTEM
# ============================================================================

from dataclasses import dataclass, field
from datetime import datetime
import uuid

@dataclass
class JobInfo:
    """Represents an active or queued download job."""
    job_id: str
    account: str
    bot_type: str
    url: str
    status: str  # 'queued', 'processing', 'uploading', 'completed', 'failed'
    started_at: datetime
    completed_at: Optional[datetime] = None
    error: Optional[str] = None

# Active jobs tracked by job_id
active_jobs: Dict[str, JobInfo] = {}

# Queue counts per account/bot combination
queue_counts: Dict[str, int] = {}  # "account:bot" -> number of jobs in queue

def get_queue_status() -> dict:
    """Get current queue status for all account/bot combinations."""
    status = {
        "total_active": len([j for j in active_jobs.values() if j.status == 'processing']),
        "total_queued": sum(max(0, count - 1) for count in queue_counts.values()),
        "total_completed": len([j for j in active_jobs.values() if j.status == 'completed']),
        "accounts": {}
    }
    
    # Group by account
    for account in telegram_clients.keys():
        account_status = {"bots": {}}
        for bot in MUSIC_BOTS.keys():
            key = f"{account}:{bot}"
            count = queue_counts.get(key, 0)
            active = [j for j in active_jobs.values() 
                      if j.account == account and j.bot_type == bot and j.status == 'processing']
            queued = [j for j in active_jobs.values() 
                      if j.account == account and j.bot_type == bot and j.status == 'queued']
            
            account_status["bots"][bot] = {
                "active": len(active),
                "queued": len(queued),
                "current_job": active[0].job_id if active else None
            }
        status["accounts"][account] = account_status
    
    return status

def create_job(account: str, bot_type: str, url: str) -> JobInfo:
    """Create and register a new job."""
    job_id = str(uuid.uuid4())[:8]
    job = JobInfo(
        job_id=job_id,
        account=account,
        bot_type=bot_type,
        url=url,
        status='queued',
        started_at=datetime.now()
    )
    active_jobs[job_id] = job
    
    # Increment queue count
    key = f"{account}:{bot_type}"
    queue_counts[key] = queue_counts.get(key, 0) + 1
    
    print(f"📥 Job {job_id} created: {account}:{bot_type} (queue: {queue_counts[key]})")
    return job

def update_job_status(job_id: str, status: str, error: str = None):
    """Update job status."""
    if job_id in active_jobs:
        active_jobs[job_id].status = status
        if error:
            active_jobs[job_id].error = error
        if status in ('completed', 'failed'):
            active_jobs[job_id].completed_at = datetime.now()
        print(f"📊 Job {job_id} status: {status}")

def complete_job(job_id: str, success: bool = True, error: str = None):
    """Mark job as completed and decrement queue."""
    if job_id in active_jobs:
        job = active_jobs[job_id]
        job.status = 'completed' if success else 'failed'
        job.completed_at = datetime.now()
        if error:
            job.error = error
        
        # Decrement queue count
        key = f"{job.account}:{job.bot_type}"
        if key in queue_counts and queue_counts[key] > 0:
            queue_counts[key] -= 1
        
        print(f"✅ Job {job_id} {'completed' if success else 'failed'}: {key} (queue: {queue_counts.get(key, 0)})")

def cleanup_old_jobs(max_age_seconds: int = 3600):
    """Remove completed/failed jobs older than max_age."""
    now = datetime.now()
    to_remove = []
    for job_id, job in active_jobs.items():
        if job.completed_at:
            age = (now - job.completed_at).total_seconds()
            if age > max_age_seconds:
                to_remove.append(job_id)
    for job_id in to_remove:
        del active_jobs[job_id]
    if to_remove:
        print(f"🧹 Cleaned up {len(to_remove)} old jobs")

# Account configurations
ACCOUNTS = {
    '1': {'phone': '+212694375170', 'name': 'Account 1'},
    '2': {'phone': '+212648243962', 'name': 'Account 2'}
}


def load_account_config():
    """Load account configurations from environment variables."""
    accounts = {}
    
    # Load primary account (Account 1)
    api_id = os.getenv('TELEGRAM_API_ID')
    api_hash = os.getenv('TELEGRAM_API_HASH')
    if api_id and api_hash:
        accounts['1'] = {
            'api_id': int(api_id),
            'api_hash': api_hash,
            'phone': os.getenv('TELEGRAM_PHONE', ACCOUNTS['1']['phone']),
            'name': 'Account 1'
        }
    
    # Load additional accounts (Account 2, 3, etc.)
    for i in range(2, 11):  # Support up to 10 accounts
        api_id = os.getenv(f'TELEGRAM_API_ID_{i}')
        api_hash = os.getenv(f'TELEGRAM_API_HASH_{i}')
        if api_id and api_hash:
            accounts[str(i)] = {
                'api_id': int(api_id),
                'api_hash': api_hash,
                'phone': os.getenv(f'TELEGRAM_PHONE_{i}', ACCOUNTS.get(str(i), {}).get('phone', '')),
                'name': f'Account {i}'
            }
    
    return accounts


async def initialize_telegram_clients():
    """Initialize and connect all Telegram clients."""
    global telegram_clients
    
    accounts_config = load_account_config()
    
    if not accounts_config:
        print("⚠ No Telegram credentials found in environment variables")
        return
    
    for account_name, config in accounts_config.items():
        session_name = f"music_bot_session_{account_name}"
        
        try:
            client = Client(
                name=session_name,
                api_id=config['api_id'],
                api_hash=config['api_hash'],
                workdir=SESSIONS_DIR
            )
            
            await client.start()
            
            # Add message handler for instant response detection
            async def on_bot_message(client, message, acc_name=account_name):
                """Handler for incoming bot messages - triggers waiting downloads instantly."""
                print(f"🔔 [HANDLER] Message received on account {acc_name}")
                print(f"   from_user: {message.from_user}")
                
                if not message.from_user or not message.from_user.is_bot:
                    print(f"   ❌ Not from a bot, ignoring")
                    return
                
                bot_username = message.from_user.username
                print(f"   bot_username: {bot_username}")
                print(f"   has audio: {message.audio is not None}")
                print(f"   pending_responses keys: {list(pending_responses.keys())}")
                
                if not bot_username:
                    return
                
                # Check if we're waiting for this bot's response
                wait_key = f"{acc_name}:{bot_username}"
                print(f"   wait_key to check: {wait_key}")
                
                if wait_key in pending_responses:
                    print(f"   ✅ Found matching wait_key!")
                    pending = pending_responses[wait_key]
                    # Store the message and signal the waiting coroutine
                    if message.audio or (message.text and any(err in message.text.lower() for err in ['error', 'not found', 'unavailable', 'failed'])):
                        print(f"   ✅ Setting event for audio/error message")
                        pending['message'] = message
                        pending['event'].set()
                else:
                    print(f"   ⚠️ wait_key not found in pending_responses")
            
            client.add_handler(MessageHandler(on_bot_message))
            telegram_clients[account_name] = client
            print(f"✓ {config['name']} ({config['phone']}) connected")
            
        except Exception as e:
            print(f"✗ Failed to connect {config['name']}: {e}")
    
    if len(telegram_clients) == 0:
        print("⚠ No Telegram accounts connected!")
    else:
        print(f"✓ {len(telegram_clients)} account(s) ready")


async def disconnect_all_clients():
    """Disconnect all Telegram clients."""
    global telegram_clients
    
    for account_name, client in telegram_clients.items():
        try:
            if client.is_connected:
                await client.stop()
            print(f"✓ {account_name} disconnected")
        except Exception as e:
            print(f"✗ Error disconnecting {account_name}: {e}")
    
    telegram_clients.clear()


def get_telegram_client(account_name: Optional[str] = None, strategy: str = "least_busy") -> tuple[Client, str]:
    """Get a Telegram client with smart rotation based on pending requests."""
    global current_account_index, pending_requests, account_locks
    
    if not telegram_clients:
        raise Exception("No Telegram accounts available. Please initialize clients first.")
    
    # Initialize locks and pending counts for new accounts
    for acc in telegram_clients.keys():
        if acc not in account_locks:
            account_locks[acc] = Lock()
        if acc not in pending_requests:
            pending_requests[acc] = 0
    
    # If specific account requested
    if account_name and account_name in telegram_clients:
        pending_requests[account_name] += 1
        print(f"📌 Using requested Account {account_name} (pending: {pending_requests[account_name]})")
        return telegram_clients[account_name], account_name
    
    # LEAST BUSY: Pick account with fewest pending requests (prevents overload!)
    if strategy == "least_busy":
        # Find account with minimum pending requests
        min_pending = float('inf')
        selected_account = None
        for acc in telegram_clients.keys():
            count = pending_requests.get(acc, 0)
            if count < min_pending:
                min_pending = count
                selected_account = acc
        
        if selected_account is None:
            selected_account = list(telegram_clients.keys())[0]
        
        pending_requests[selected_account] += 1
        print(f"⚖️ Using Account {selected_account} (least busy: {pending_requests[selected_account]} pending)")
        return telegram_clients[selected_account], selected_account
    
    # Round-robin rotation (fallback)
    elif strategy == "round_robin":
        account_names = list(telegram_clients.keys())
        selected_account = account_names[current_account_index]
        client = telegram_clients[selected_account]
        current_account_index = (current_account_index + 1) % len(account_names)
        pending_requests[selected_account] += 1
        print(f"🔄 Using Account {selected_account} (round-robin, pending: {pending_requests[selected_account]})")
        return client, selected_account
    
    # Failover: try account 1 first
    else:
        if '1' in telegram_clients:
            pending_requests['1'] += 1
            print(f"💾 Using Account 1 (failover)")
            return telegram_clients['1'], '1'
        selected_account = list(telegram_clients.keys())[0]
        pending_requests[selected_account] += 1
        print(f"💾 Using Account {selected_account} (failover)")
        return telegram_clients[selected_account], selected_account


def release_account(account_name: str):
    """Release account after request completion (decrement pending count)."""
    global pending_requests
    if account_name in pending_requests and pending_requests[account_name] > 0:
        pending_requests[account_name] -= 1
        print(f"✅ Released Account {account_name} (pending now: {pending_requests[account_name]})")


def get_total_accounts() -> int:
    """Get total number of connected accounts."""
    return len(telegram_clients)


# ============================================================================
# CLOUDFLARE R2 FUNCTIONS (OPTIMIZED)
# ============================================================================

def upload_bytes_to_r2(file_bytes: BytesIO, object_key: str, file_size: int = 0) -> str:
    """Upload file from memory directly to R2 using pre-initialized client."""
    
    size_mb = file_size / (1024 * 1024) if file_size > 0 else 0
    print(f"☁️  Uploading {size_mb:.2f} MB to R2...")
    
    start_time = time.time()
    client = get_s3_client()
    
    # Upload from memory with optimized transfer config
    file_bytes.seek(0)
    
    # For large files (>10MB), use multipart upload
    if file_size > 10 * 1024 * 1024:
        from boto3.s3.transfer import TransferConfig
        config = TransferConfig(
            multipart_threshold=8 * 1024 * 1024,  # 8MB threshold
            max_concurrency=10,  # Parallel uploads
            multipart_chunksize=8 * 1024 * 1024,  # 8MB chunks
            use_threads=True
        )
        client.upload_fileobj(
            file_bytes,
            R2_BUCKET_NAME,
            object_key,
            ExtraArgs={'ContentType': 'audio/flac'},
            Config=config
        )
    else:
        client.upload_fileobj(
            file_bytes,
            R2_BUCKET_NAME,
            object_key,
            ExtraArgs={'ContentType': 'audio/flac'}
        )
    
    elapsed = time.time() - start_time
    speed = size_mb / elapsed if elapsed > 0 else 0
    public_url = f"{R2_PUBLIC_URL}/{object_key}"
    print(f"✓ Uploaded in {elapsed:.1f}s ({speed:.1f} MB/s)")
    return public_url


def schedule_file_deletion(object_key: str, delay_seconds: int = 300):
    """Schedule a file to be deleted after delay_seconds."""
    
    def delete_after_delay():
        time.sleep(delay_seconds)
        try:
            get_s3_client().delete_object(Bucket=R2_BUCKET_NAME, Key=object_key)
            print(f"🗑️  Auto-deleted: {object_key}")
        except Exception as e:
            print(f"✗ Failed to delete {object_key}: {e}")
    
    # Start background thread
    thread = threading.Thread(target=delete_after_delay, daemon=True)
    thread.start()
    print(f"⏰ Scheduled deletion in {delay_seconds}s")


# ============================================================================
# TELEGRAM DOWNLOAD FUNCTION (STREAMING - PARALLEL DOWNLOAD/UPLOAD)
# ============================================================================

async def stream_to_r2(telegram_client: Client, audio_message, object_key: str) -> str:
    """Stream directly from Telegram to R2 using multipart upload."""
    
    file_size = audio_message.audio.file_size
    size_mb = file_size / (1024 * 1024)
    
    print(f"🚀 Streaming {size_mb:.1f}MB directly to R2...")
    start_time = time.time()
    
    client = get_s3_client()
    
    # Start multipart upload
    mpu = client.create_multipart_upload(
        Bucket=R2_BUCKET_NAME,
        Key=object_key,
        ContentType='audio/flac'
    )
    upload_id = mpu['UploadId']
    
    parts = []
    part_number = 1
    buffer = BytesIO()
    chunk_size = 5 * 1024 * 1024  # 5MB minimum for multipart (R2/S3 requirement)
    bytes_transferred = 0
    
    try:
        # Stream chunks from Telegram
        async for chunk in telegram_client.stream_media(audio_message):
            buffer.write(chunk)
            bytes_transferred += len(chunk)
            
            # When buffer reaches chunk_size, upload as a part
            if buffer.tell() >= chunk_size:
                buffer.seek(0)
                
                # Upload this part
                part = client.upload_part(
                    Bucket=R2_BUCKET_NAME,
                    Key=object_key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=buffer.read()
                )
                
                parts.append({
                    'PartNumber': part_number,
                    'ETag': part['ETag']
                })
                
                progress = (bytes_transferred / file_size) * 100
                print(f"   📤 Part {part_number} uploaded ({progress:.0f}%)")
                
                part_number += 1
                buffer = BytesIO()  # Reset buffer
        
        # Upload remaining data (final part can be smaller than 5MB)
        if buffer.tell() > 0:
            buffer.seek(0)
            part = client.upload_part(
                Bucket=R2_BUCKET_NAME,
                Key=object_key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=buffer.read()
            )
            parts.append({
                'PartNumber': part_number,
                'ETag': part['ETag']
            })
            print(f"   📤 Part {part_number} uploaded (100%)")
        
        # Complete multipart upload
        client.complete_multipart_upload(
            Bucket=R2_BUCKET_NAME,
            Key=object_key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
        
        elapsed = time.time() - start_time
        speed = size_mb / elapsed if elapsed > 0 else 0
        print(f"✓ Streamed in {elapsed:.1f}s ({speed:.1f} MB/s) - NO intermediate storage!")
        
        return f"{R2_PUBLIC_URL}/{object_key}"
        
    except Exception as e:
        # Abort multipart upload on error
        client.abort_multipart_upload(
            Bucket=R2_BUCKET_NAME,
            Key=object_key,
            UploadId=upload_id
        )
        raise e


async def download_from_telegram(url: str, bot_type: str, account_name: str = None) -> dict:
    """Download file from Telegram bot with account rotation and streaming upload.
    
    Uses per-account-per-bot locks to prevent concurrent requests to the SAME bot
    on the SAME account, which would cause message polling conflicts.
    
    NOTE: Same account CAN handle concurrent requests to DIFFERENT bots!
    Example: Account 1 → Amazon + Account 1 → Deezer = OK (different bots)
             Account 1 → Amazon + Account 1 → Amazon = QUEUED (same bot)
    """
    bot_username = MUSIC_BOTS.get(bot_type, MUSIC_BOTS['amazon'])
    
    # Get client with rotation (picks least busy account)
    telegram_client, used_account = get_telegram_client(account_name, strategy="least_busy")
    print(f"🔄 Account {used_account} → @{bot_username}")
    
    # Create job for tracking
    job = create_job(used_account, bot_type, url)
    job_id = job.job_id
    
    # Lock key is account + bot combination (allows same account to use different bots concurrently)
    lock_key = f"{used_account}:{bot_type}"
    if lock_key not in account_locks:
        account_locks[lock_key] = Lock()
    account_lock = account_locks[lock_key]
    
    # Create wait key for event-driven response
    wait_key = f"{used_account}:{bot_username}"
    
    # Acquire lock - this ensures only ONE request per account PER BOT at a time
    async with account_lock:
        try:
            update_job_status(job_id, 'processing')
            print(f"🔒 Acquired lock for {lock_key}")
            
            # Set up event BEFORE sending message (so handler can find it)
            response_event = Event()
            pending_responses[wait_key] = {
                'event': response_event,
                'message': None,
                'error': None
            }
            print(f"📝 Registered wait_key: {wait_key}")
            
            # Send URL to bot
            print(f"📤 Sending URL...")
            send_time = time.time()
            message = await telegram_client.send_message(bot_username, url)
            
            # Wait for handler to signal response (timeout 60 seconds)
            print(f"⏳ Waiting for bot response via event handler...")
            try:
                await asyncio.wait_for(response_event.wait(), timeout=60.0)
            except asyncio.TimeoutError:
                complete_job(job_id, success=False, error='Timeout: No audio file received')
                return {'success': False, 'job_id': job_id, 'error': 'Timeout: No audio file received'}
            
            response_time = time.time() - send_time
            print(f"✅ Event received! Response time: {response_time:.2f}s")
            
            # Get the response from pending_responses
            response_data = pending_responses.get(wait_key, {})
            msg = response_data.get('message')
            error = response_data.get('error')
            
            if error:
                complete_job(job_id, success=False, error=error)
                return {'success': False, 'job_id': job_id, 'error': error}
            
            if msg and msg.audio:
                size_mb = msg.audio.file_size / (1024 * 1024)
                print(f"📦 File ready: {size_mb:.1f}MB - {msg.audio.performer} - {msg.audio.title}")
                
                # Cache file reference for streaming (skip R2 upload)
                file_unique_id = msg.audio.file_unique_id
                file_cache[file_unique_id] = {
                    'account': used_account,
                    'message': msg,
                    'expires': time.time() + FILE_CACHE_TTL
                }
                
                # Return streaming URL (instant - no upload)
                stream_url = f"http://localhost:8000/stream/{file_unique_id}"
                print(f"⚡ Instant response - streaming URL ready")
                
                complete_job(job_id, success=True)
                return {
                    'success': True,
                    'job_id': job_id,
                    'public_url': stream_url,
                    'object_key': file_unique_id,
                    'performer': msg.audio.performer,
                    'title': msg.audio.title,
                    'duration': msg.audio.duration,
                    'file_size': msg.audio.file_size
                }
            
            complete_job(job_id, success=False, error='No audio in response')
            return {'success': False, 'job_id': job_id, 'error': 'No audio in response'}
            
        except Exception as e:
            complete_job(job_id, success=False, error=str(e))
            return {'success': False, 'job_id': job_id, 'error': str(e)}
        
        finally:
            # Clean up pending response
            if wait_key in pending_responses:
                del pending_responses[wait_key]
            # Always release account when done
            release_account(used_account)
            print(f"🔓 Released lock for {lock_key}")


# ============================================================================
# FASTAPI APPLICATION
# ============================================================================

app = FastAPI(
    title="Music Download API",
    description="Download music from Amazon Music, Deezer, and Apple Music",
    version="2.0.0"
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Pydantic models
class DownloadRequest(BaseModel):
    url: str
    platform: str  # 'amazon', 'deezer', or 'apple'
    account: Optional[str] = None  # Optional: specify account '1', '2', etc.


class DownloadResponse(BaseModel):
    success: bool
    download_url: Optional[str] = None
    artist: Optional[str] = None
    title: Optional[str] = None
    duration: Optional[int] = None
    file_size: Optional[int] = None
    error: Optional[str] = None


class AuthStartRequest(BaseModel):
    account_number: str  # '1', '2', etc.
    phone: str  # '+212694375170'


class AuthCodeRequest(BaseModel):
    account_number: str
    code: str  # Code received via Telegram/SMS


# Startup and shutdown events
@app.on_event("startup")
async def startup_event():
    """Initialize Telegram accounts and S3 client on startup."""
    try:
        # Pre-initialize S3 client (saves ~500ms on first request)
        print("☁️  Pre-warming R2 connection...")
        get_s3_client()
        print("✓ R2 client ready")
        
        await initialize_telegram_clients()
        total = get_total_accounts()
        print(f"✓ API ready with {total} Telegram account(s)")
        print(f"✓ Rotation strategy: {ROTATION_STRATEGY}")
    except Exception as e:
        print(f"✗ Failed to initialize: {e}")
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """Disconnect all Telegram clients on shutdown."""
    try:
        await disconnect_all_clients()
        print("✓ All Telegram clients disconnected")
    except Exception as e:
        print(f"✗ Error during shutdown: {e}")


# API endpoints
@app.get("/")
async def root():
    """API information."""
    total_accounts = get_total_accounts()
    
    return {
        "name": "Music Download API",
        "version": "2.0.0",
        "telegram_accounts": total_accounts,
        "rotation_strategy": ROTATION_STRATEGY,
        "supported_platforms": list(MUSIC_BOTS.keys()),
        "endpoints": {
            "POST /download": "Download music and get R2 URL",
            "GET /queue": "View queue status for all account/bot combinations",
            "GET /jobs": "View all active and recent jobs",
            "GET /health": "Health check"
        }
    }


@app.get("/queue")
async def queue_status():
    """Get current queue status for all account/bot combinations."""
    cleanup_old_jobs()  # Clean up old jobs periodically
    return get_queue_status()


@app.get("/jobs")
async def list_jobs():
    """List all active and recent jobs."""
    jobs = []
    for job_id, job in active_jobs.items():
        jobs.append({
            "job_id": job.job_id,
            "account": job.account,
            "bot": job.bot_type,
            "status": job.status,
            "started_at": job.started_at.isoformat(),
            "completed_at": job.completed_at.isoformat() if job.completed_at else None,
            "error": job.error
        })
    
    # Sort by started_at descending
    jobs.sort(key=lambda x: x["started_at"], reverse=True)
    
    return {
        "total": len(jobs),
        "active": len([j for j in jobs if j["status"] == "processing"]),
        "queued": len([j for j in jobs if j["status"] == "queued"]),
        "completed": len([j for j in jobs if j["status"] == "completed"]),
        "failed": len([j for j in jobs if j["status"] == "failed"]),
        "jobs": jobs[:50]  # Limit to last 50 jobs
    }


@app.post("/download", response_model=DownloadResponse)
async def download_music(request: DownloadRequest, background_tasks: BackgroundTasks):
    """
    Download music from URL and return CloudFlare R2 download link.
    
    Supported platforms:
    - amazon: Amazon Music
    - deezer: Deezer
    - apple: Apple Music
    """
    # Validate platform
    if request.platform not in MUSIC_BOTS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid platform. Supported: {list(MUSIC_BOTS.keys())}"
        )
    
    # Download from Telegram bot (streams directly to R2)
    # Passes optional account parameter for load balancing
    result = await download_from_telegram(request.url, request.platform, request.account)
    
    if not result['success']:
        return DownloadResponse(
            success=False,
            error=result.get('error', 'Download failed')
        )
    
    try:
        # Streaming mode - no R2 upload, URL points to our /stream endpoint
        return DownloadResponse(
            success=True,
            download_url=result['public_url'],
            artist=result['performer'],
            title=result['title'],
            duration=result['duration'],
            file_size=result['file_size']
        )
        
    except Exception as e:
        return DownloadResponse(
            success=False,
            error=f"Failed: {str(e)}"
        )


@app.get("/stream/{file_unique_id}")
async def stream_file(file_unique_id: str):
    """Stream audio file directly from Telegram (no R2 storage)."""
    
    # Check cache
    if file_unique_id not in file_cache:
        raise HTTPException(404, "File not found or expired")
    
    cache_entry = file_cache[file_unique_id]
    
    # Check expiry
    if time.time() > cache_entry['expires']:
        del file_cache[file_unique_id]
        raise HTTPException(410, "File link expired")
    
    # Get the Telegram client for this account
    account_name = cache_entry['account']
    if account_name not in telegram_clients:
        raise HTTPException(503, "Telegram account unavailable")
    
    client = telegram_clients[account_name]
    message = cache_entry['message']
    
    async def stream_generator():
        """Stream chunks from Telegram."""
        async for chunk in client.stream_media(message):
            yield chunk
    
    # Get filename for Content-Disposition
    filename = f"{message.audio.performer} - {message.audio.title}.flac"
    filename = filename.replace('"', "'")  # Escape quotes
    
    return StreamingResponse(
        stream_generator(),
        media_type="audio/flac",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(message.audio.file_size)
        }
    )


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "accounts": get_total_accounts(),
        "rotation": ROTATION_STRATEGY,
        "cached_files": len(file_cache)
    }


# ============================================================================
# AUTHENTICATION ENDPOINTS
# ============================================================================

@app.post("/auth/start")
async def auth_start(request: AuthStartRequest):
    """Start authentication process for a Telegram account."""
    try:
        # Get account config
        accounts_config = load_account_config()
        
        if request.account_number not in accounts_config:
            raise HTTPException(400, f"Account {request.account_number} not configured in environment variables")
        
        config = accounts_config[request.account_number]
        session_name = f"music_bot_session_{request.account_number}"
        
        # Create client
        client = Client(
            name=session_name,
            api_id=config['api_id'],
            api_hash=config['api_hash'],
            phone_number=request.phone,
            workdir=SESSIONS_DIR
        )
        
        # Start and send code
        await client.connect()
        sent_code = await client.send_code(request.phone)
        
        # Store client temporarily (don't add to main pool yet)
        telegram_clients[f"temp_{request.account_number}"] = client
        
        return {
            "success": True,
            "message": f"Code sent to {request.phone}",
            "phone_code_hash": sent_code.phone_code_hash,
            "account_number": request.account_number
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/auth/code")
async def auth_code(request: AuthCodeRequest):
    """Submit verification code to complete authentication."""
    try:
        temp_key = f"temp_{request.account_number}"
        
        if temp_key not in telegram_clients:
            raise HTTPException(400, "Authentication not started. Call /auth/start first.")
        
        client = telegram_clients[temp_key]
        
        # Sign in with code
        await client.sign_in(
            phone_number=client.phone_number,
            phone_code_hash=client.phone_code_hash,
            phone_code=request.code
        )
        
        # Move to main pool
        telegram_clients[request.account_number] = client
        del telegram_clients[temp_key]
        
        return {
            "success": True,
            "message": f"Account {request.account_number} authenticated successfully",
            "total_accounts": get_total_accounts()
        }
        
    except Exception as e:
        # Clean up temp client
        temp_key = f"temp_{request.account_number}"
        if temp_key in telegram_clients:
            try:
                await telegram_clients[temp_key].disconnect()
            except:
                pass
            del telegram_clients[temp_key]
        
        return {"success": False, "error": str(e)}


@app.get("/auth/status")
async def auth_status():
    """Check which accounts are authenticated."""
    accounts_config = load_account_config()
    
    status = {}
    for account_num in accounts_config.keys():
        status[f"Account {account_num}"] = {
            "authenticated": account_num in telegram_clients,
            "phone": accounts_config[account_num]['phone']
        }
    
    return {
        "total_configured": len(accounts_config),
        "total_authenticated": get_total_accounts(),
        "accounts": status
    }


# Run server
if __name__ == "__main__":
    print("="*60)
    print("Music Download API Server - All-in-One")
    print("="*60)
    print(f"Supported platforms: {list(MUSIC_BOTS.keys())}")
    print(f"Starting server on http://0.0.0.0:8000")
    print("="*60)
    
    uvicorn.run(app, host="0.0.0.0", port=8000)
