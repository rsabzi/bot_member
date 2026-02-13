import paramiko
import requests
import uuid
import json
import secrets
import time
import traceback
import os
from dotenv import load_dotenv

load_dotenv()

# Server Details
HOST = os.getenv("SSH_HOST")
USER = os.getenv("SSH_USER")
PASS = os.getenv("SSH_PASSWORD")
PORT = int(os.getenv("SSH_PORT", 22))
REMOTE_BASE_DIR = "/home/sabzi/irontunnel" # Keeping this as it might be specific to this script's purpose
API_URL = f"http://{HOST}:8000"

if not all([HOST, USER, PASS]):
    print("❌ Error: Missing SSH credentials in .env")
    exit(1)

def create_ssh_client():
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(HOST, port=PORT, username=USER, password=PASS)
    return client

def exec_command(client, command, print_output=True, sudo=False):
    if sudo:
        command = f"echo '{PASS}' | sudo -S {command}"
    
    # print(f"Executing: {command.replace(PASS, '******')}")
    stdin, stdout, stderr = client.exec_command(command)
    exit_status = stdout.channel.recv_exit_status()
    
    out_str = stdout.read().decode().strip()
    err_str = stderr.read().decode().strip()
    
    if print_output and out_str:
        print(out_str)
    if print_output and err_str:
        # Filter out sudo prompt if present
        if "[sudo] password" not in err_str or len(err_str.splitlines()) > 1:
             print(f"Error: {err_str}")
             
    return out_str

def main():
    try:
        print(f"Connecting to {HOST}...")
        client = create_ssh_client()
        
        # 1. Update API and Service Files
        print("Updating API and Service files...")
        sftp = client.open_sftp()
        # Note: deploy_irontunnel.py likely uploaded contents of marzban_setup to root of irontunnel
        # So remote path is irontunnel/api/main.py, not irontunnel/marzban_setup/api/main.py
        sftp.put("marzban_setup/api/main.py", f"{REMOTE_BASE_DIR}/api/main.py")
        sftp.put("sing-box.service", f"{REMOTE_BASE_DIR}/sing-box.service")
        sftp.close()
        
        # 2. Restart API to apply changes
        print("Restarting IronTunnel API...")
        exec_command(client, "systemctl restart irontunnel", sudo=True)
        time.sleep(5) # Wait for API to come back up

        # 3. Setup Sing-box Service
        print("Setting up Sing-box Systemd Service...")
        exec_command(client, f"mv {REMOTE_BASE_DIR}/sing-box.service /etc/systemd/system/sing-box.service", sudo=True)
        exec_command(client, "systemctl daemon-reload", sudo=True)
        exec_command(client, "mkdir -p /var/lib/sing-box", sudo=True)
        exec_command(client, "systemctl enable sing-box", sudo=True)

        # 4. Generate Keys on Server
        print("Generating Keys and Certificates...")
        
        # Generate Reality Key Pair
        # Using sing-box generate reality-keypair (if available) or xray
        # Since we installed sing-box, let's try using it, but syntax varies.
        # Safest is to use python to generate x25519 keys or just use the pre-installed sing-box if version supports 'generate'
        # Let's use a small python script on remote to generate keys using 'curve25519-dalek' or just rely on 'openssl' or 'sing-box'
        
        # Try sing-box generate
        # output format: PrivateKey: ... PublicKey: ...
        keys_out = exec_command(client, "/usr/local/bin/sing-box generate reality-keypair")
        if "PrivateKey" in keys_out:
            private_key = keys_out.split("PrivateKey: ")[1].split("\n")[0].strip()
            public_key = keys_out.split("PublicKey: ")[1].split("\n")[0].strip()
        else:
            # Fallback or error
            print("Failed to generate keys via sing-box, using python fallback...")
            # This is complex without libraries. Let's assume sing-box works as it is v1.8
            raise Exception("Could not generate Reality keys")

        print(f"Reality Keys Generated.")
        
        # Generate Self-Signed Cert for Hysteria2
        cert_dir = "/var/lib/sing-box/certs"
        exec_command(client, f"mkdir -p {cert_dir}", sudo=True)
        
        cmd_cert = (
            f"openssl req -x509 -newkey rsa:4096 -keyout {cert_dir}/hy2.key "
            f"-out {cert_dir}/hy2.crt -sha256 -days 3650 -nodes "
            f"-subj '/C=US/ST=Oregon/L=Portland/O=Company Name/OU=Org/CN=www.microsoft.com'"
        )
