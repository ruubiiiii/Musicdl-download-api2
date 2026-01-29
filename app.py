"""
MusicDL API Server - Complete in One File
Supports: Amazon Music, Deezer, Apple Music
Features: Account rotation, CloudFlare R2 upload, auto-deletion
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pyrogram import Client
import boto3
from botocore.config import Config
import asyncio
import os
from typing import Optional, Dict
from io import BytesIO
import uvicorn
import tempfile
import threading
import time
import concurrent.futures

# Thread pool for parallel operations
upload_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

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


def get_telegram_client(account_name: Optional[str] = None, strategy: str = "round_robin") -> tuple[Client, str]:
    """Get a Telegram client with rotation."""
    global current_account_index
    
    if not telegram_clients:
        raise Exception("No Telegram accounts available. Please initialize clients first.")
    
    # If specific account requested
    if account_name and account_name in telegram_clients:
        return telegram_clients[account_name], account_name
    
    # Round-robin rotation (default)
    if strategy == "round_robin":
        account_names = list(telegram_clients.keys())
        selected_account = account_names[current_account_index]
        client = telegram_clients[selected_account]
        current_account_index = (current_account_index + 1) % len(account_names)
        print(f"🔄 Using Account {selected_account} (round-robin {current_account_index}/{len(account_names)})")
        return client, selected_account
    
    # Random selection
    elif strategy == "random":
        import random
        selected_account = random.choice(list(telegram_clients.keys()))
        print(f"🎲 Using Account {selected_account} (random)")
        return telegram_clients[selected_account], selected_account
    
    # Failover: try account 1 first, then any available
    else:
        if '1' in telegram_clients:
            print(f"💾 Using Account 1 (failover)")
            return telegram_clients['1'], '1'
        selected_account = list(telegram_clients.keys())[0]
        print(f"💾 Using Account {selected_account} (failover)")
        return list(telegram_clients.values())[0], selected_account


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
    """Download file from Telegram bot with account rotation and streaming upload."""
    bot_username = MUSIC_BOTS.get(bot_type, MUSIC_BOTS['amazon'])
    
    # Get client with rotation
    telegram_client, used_account = get_telegram_client(account_name, strategy=ROTATION_STRATEGY)
    print(f"🔄 Account {used_account} → @{bot_username}")
    
    try:
        # Send URL to bot
        print(f"📤 Sending URL...")
        message = await telegram_client.send_message(bot_username, url)
        
        # Wait for audio file (max 60 seconds)
        print(f"⏳ Waiting for bot response...")
        for attempt in range(120):
            await asyncio.sleep(0.5)
            
            async for msg in telegram_client.get_chat_history(bot_username, limit=10):
                if msg.from_user and msg.from_user.is_bot and msg.date >= message.date:
                    if msg.audio:
                        size_mb = msg.audio.file_size / (1024 * 1024)
                        print(f"📦 File ready: {size_mb:.1f}MB - {msg.audio.performer} - {msg.audio.title}")
                        
                        # Generate object key
                        object_key = f"music/{msg.audio.file_unique_id}.flac"
                        
                        # Stream directly from Telegram to R2
                        public_url = await stream_to_r2(telegram_client, msg, object_key)
                        
                        return {
                            'success': True,
                            'public_url': public_url,
                            'object_key': object_key,
                            'performer': msg.audio.performer,
                            'title': msg.audio.title,
                            'duration': msg.audio.duration,
                            'file_size': msg.audio.file_size
                        }
        
        return {'success': False, 'error': 'Timeout: No audio file received'}
        
    except Exception as e:
        return {'success': False, 'error': str(e)}


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
            "GET /health": "Health check"
        }
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
    result = await download_from_telegram(request.url, request.platform)
    
    if not result['success']:
        return DownloadResponse(
            success=False,
            error=result.get('error', 'Download failed')
        )
    
    try:
        # URL already created during streaming, just schedule deletion
        schedule_file_deletion(result['object_key'], delay_seconds=300)
        
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


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "accounts": get_total_accounts(),
        "rotation": ROTATION_STRATEGY
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
