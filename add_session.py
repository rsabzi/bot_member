from telethon import TelegramClient
import os
import socks
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

api_id = os.getenv("API_ID")
api_hash = os.getenv("API_HASH")

if not api_id or not api_hash:
    print("Please set API_ID and API_HASH in .env file first.")
    exit(1)

def create_session():
    print("=========================================")
    print("   Telegram Session Creator (Proxy)      ")
    print("=========================================")
    
    # Config from .env
    PROXY_HOST = os.getenv("PROXY_HOST", "127.0.0.1")
    PROXY_PORT = int(os.getenv("PROXY_PORT", 9050))
    proxy_type_str = os.getenv("PROXY_TYPE", "HTTP").upper()
    
    PROXY_TYPE = socks.HTTP
    if proxy_type_str == "SOCKS5":
        PROXY_TYPE = socks.SOCKS5
    elif proxy_type_str == "SOCKS4":
        PROXY_TYPE = socks.SOCKS4
    
    print(f"🌍 Using Proxy: {proxy_type_str}://{PROXY_HOST}:{PROXY_PORT}")
    print("=========================================")
    
    session_name = input("\nEnter session name (e.g., worker1): ").strip()
    if not session_name:
        print("Session name cannot be empty.")
        return

    # Configure Proxy
    proxy_config = (PROXY_TYPE, PROXY_HOST, PROXY_PORT)

    client = TelegramClient(session_name, api_id, api_hash, proxy=proxy_config)
    
    async def main():
        print(f"\n🔄 Connecting to Telegram via Proxy ({PROXY_PORT})...")
        await client.start()
        
        me = await client.get_me()
        print(f"\n✅ SUCCESS! Logged in as: {me.first_name} (ID: {me.id})")
        print(f"💾 Session file saved: {session_name}.session")
        print("Now you can upload this file to your server.")

    try:
        with client:
            client.loop.run_until_complete(main())
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nTroubleshooting:")
        print(f"1. Make sure your proxy tool is RUNNING on port {PROXY_PORT}.")
        print("2. Check your .env file for correct PROXY settings.")

if __name__ == "__main__":
    create_session()
