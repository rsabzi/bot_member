import paramiko
import os
import time
import glob
from dotenv import load_dotenv

load_dotenv()

hostname = os.getenv("SSH_HOST")
username = os.getenv("SSH_USER")
password = os.getenv("SSH_PASSWORD")
port = int(os.getenv("SSH_PORT", 22))

if not all([hostname, username, password]):
    print("❌ Error: Missing SSH credentials in .env")
    exit(1)

def deploy_and_reset():
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        if not os.path.exists(".env"):
            print("❌ Error: .env file not found.")
            return

        print(f"Connecting to {hostname}:{port}...")
        client.connect(hostname, port=port, username=username, password=password, timeout=20)
        print(f"✅ Connected successfully!")
        
        sftp = client.open_sftp()
        remote_dir = "/root/member_bot"
        
        # 1. Stop Bot
        print("🛑 Stopping bot...")
        client.exec_command("pkill -f bot.py")
        time.sleep(2)
        
        # 2. Upload Session Files
        session_files = glob.glob("*.session")
        print(f"📂 Found sessions: {session_files}")
        
        for sess in session_files:
            remote_path = f"{remote_dir}/{os.path.basename(sess)}"
            print(f"📤 Uploading {sess} -> {remote_path}")
            sftp.put(sess, remote_path)
            
        # 3. Upload Latest Code (bot.py, add_session.py)
        print("📤 Uploading latest code...")
        sftp.put("bot.py", f"{remote_dir}/bot.py")
        sftp.put("wipe_all_data.py", f"{remote_dir}/wipe_all_data.py")
        sftp.put("requirements.txt", f"{remote_dir}/requirements.txt")

        print("📤 Uploading .env...")
        with open(".env", "r") as f:
            env_lines = f.readlines()
        with open(".env.remote", "w") as f:
            for line in env_lines:
                if not line.startswith("PROXY_"):
                    f.write(line)
        sftp.put(".env.remote", f"{remote_dir}/.env")
        os.remove(".env.remote")
        
        # 4. Install new requirements (pysocks)
        print("📦 Installing requirements...")
        client.exec_command(f"cd {remote_dir} && pip3 install -r requirements.txt")

        # 5. Run Wipe Script
        print("🧹 Wiping data...")
        stdin, stdout, stderr = client.exec_command(f"cd {remote_dir} && python3 wipe_all_data.py")
        print(stdout.read().decode())
        err = stderr.read().decode()
        if err: print(f"Error (Wipe): {err}")
            
        # 6. Restart Bot
        print("🚀 Restarting bot...")
        client.exec_command(f"cd {remote_dir} && nohup python3 -u bot.py > bot.log 2>&1 &")
        print("✅ Bot restarted with new sessions!")
            
    except Exception as e:
        print(f"❌ An error occurred: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    deploy_and_reset()
