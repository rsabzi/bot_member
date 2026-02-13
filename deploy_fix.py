import paramiko
import os
import time
from dotenv import load_dotenv

load_dotenv()

hostname = os.getenv("SSH_HOST")
username = os.getenv("SSH_USER")
password = os.getenv("SSH_PASSWORD")
port = int(os.getenv("SSH_PORT", 22))

if not all([hostname, username, password]):
    print("❌ Error: Missing SSH credentials in .env")
    exit(1)

def deploy_fix():
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        print(f"Connecting to {hostname}:{port}...")
        client.connect(hostname, port=port, username=username, password=password, timeout=20)
        print(f"✅ Connected successfully!")
        
        sftp = client.open_sftp()
        remote_dir = "/root/member_bot"
        
        # 1. Stop Bot
        print("🛑 Stopping bot...")
        client.exec_command("pkill -f bot.py")
        time.sleep(2)
        
        # 2. Upload Latest Code
        print("📤 Uploading updated bot.py...")
        sftp.put("bot.py", f"{remote_dir}/bot.py")

        # Create a remote-friendly .env (without Proxy)
        print("⚙️ Generating server .env (removing proxy)...")
        with open(".env", "r") as f:
            env_lines = f.readlines()
        
        with open(".env.remote", "w") as f:
            for line in env_lines:
                if not line.startswith("PROXY_"):
                    f.write(line)
        
        print("📤 Uploading .env...")
        sftp.put(".env.remote", f"{remote_dir}/.env")
        os.remove(".env.remote")
        
        print("📤 Uploading session.session...")
        sftp.put("session.session", f"{remote_dir}/session.session")
        
        # 3. Restart Bot
        print("🚀 Restarting bot...")
        client.exec_command(f"cd {remote_dir} && nohup python3 -u bot.py > bot.log 2>&1 &")
        print("✅ Bot restarted!")
        
        # 4. Check status
        time.sleep(3)
        print("\n🔍 Checking status:")
        stdin, stdout, stderr = client.exec_command("pgrep -af bot.py")
        running = stdout.read().decode().strip()
        if running:
            print(f"✅ Process Running: {running}")
            print("\n📜 Log Output:")
            stdin, stdout, stderr = client.exec_command(f"tail -n 20 {remote_dir}/bot.log")
            print(stdout.read().decode())
        else:
            print("❌ Process failed to start. Checking logs...")
            stdin, stdout, stderr = client.exec_command(f"tail -n 20 {remote_dir}/bot.log")
            print(stdout.read().decode())
            print(stderr.read().decode())
            
    except Exception as e:
        print(f"❌ An error occurred: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    deploy_fix()
