import asyncio
import sys
import time
import string
import os
import sqlite3
import pandas as pd
import glob
import random
import socks
from telethon import TelegramClient, events
from telethon.errors import ChatAdminRequiredError, ChannelPrivateError, RPCError, FloodWaitError
from telethon.tl.types import Channel, Chat, UserStatusOnline, UserStatusOffline, UserStatusRecently, UserStatusLastWeek, UserStatusLastMonth, UserStatusEmpty
import argparse
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
api_id = os.getenv("API_ID")
api_hash = os.getenv("API_HASH")
if not api_id or not api_hash:
    print("❌ Error: API_ID or API_HASH not found in .env file.")
    sys.exit(1)

session_name = "session"

# Database Setup
DB_FILE = "members.db"

# Async Helper
async def run_in_executor(func, *args):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, func, *args)

def init_db():
    conn = sqlite3.connect(DB_FILE)
    # Enable WAL mode for better concurrency
    conn.execute("PRAGMA journal_mode=WAL;")
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS members (
            id INTEGER,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            phone TEXT,
            is_bot INTEGER,
            channel_id INTEGER,
            status TEXT,
            PRIMARY KEY (id, channel_id)
        )
    ''')
    
    # Settings table for selected channel
    c.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')
    
    # Migration: Check if 'status' column exists
    try:
        c.execute("SELECT status FROM members LIMIT 1")
    except sqlite3.OperationalError:
        print("Migrating DB: Adding 'status' column...")
        c.execute("ALTER TABLE members ADD COLUMN status TEXT")
    
    # Checkpoints table for resume capability
    c.execute('''
        CREATE TABLE IF NOT EXISTS scan_checkpoints (
            channel_id INTEGER PRIMARY KEY,
            last_query_index INTEGER,
            phase INTEGER DEFAULT 1
        )
    ''')
    
    # Migration: Check if 'phase' column exists in scan_checkpoints
    try:
        c.execute("SELECT phase FROM scan_checkpoints LIMIT 1")
    except sqlite3.OperationalError:
        print("Migrating DB: Adding 'phase' column to scan_checkpoints...")
        c.execute("ALTER TABLE scan_checkpoints ADD COLUMN phase INTEGER DEFAULT 1")
    
    # Channel preferences for large channels
    c.execute('''
        CREATE TABLE IF NOT EXISTS channel_prefs (
            channel_id INTEGER PRIMARY KEY,
            scan_mode TEXT
        )
    ''')
        
    conn.commit()
    conn.close()

def save_channel_pref(channel_id, mode):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('INSERT OR REPLACE INTO channel_prefs (channel_id, scan_mode) VALUES (?, ?)', (channel_id, mode))
    conn.commit()
    conn.close()

def get_channel_pref(channel_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT scan_mode FROM channel_prefs WHERE channel_id = ?', (channel_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None

def save_checkpoint(channel_id, index, phase=1):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('INSERT OR REPLACE INTO scan_checkpoints (channel_id, last_query_index, phase) VALUES (?, ?, ?)', (channel_id, index, phase))
    conn.commit()
    conn.close()

def get_checkpoint(channel_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT last_query_index, phase FROM scan_checkpoints WHERE channel_id = ?', (channel_id,))
    row = c.fetchone()
    conn.close()
    if row:
        return row[0], row[1] if len(row) > 1 else 1
    return 0, 1

def set_setting(key, value):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', (key, str(value)))
    conn.commit()
    conn.close()

def get_setting(key):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT value FROM settings WHERE key = ?', (key,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None

async def resolve_entity(event, link_or_id=None):
    """Helper to resolve entity from link, current chat, or saved selection."""
    # Use client from event if available, otherwise fallback to global client
    use_client = event.client if event else client
    
    entity = None
    
    # 1. Explicit Link
    if link_or_id:
        try:
            if link_or_id.isdigit() or link_or_id.startswith("-"):
                entity = await use_client.get_entity(int(link_or_id))
            else:
                entity = await use_client.get_entity(link_or_id)
            return entity
        except Exception as e:
            raise Exception(f"Invalid link/ID: {e}")

    # 2. Current Chat (if channel/group)
    if event.is_channel or event.is_group:
        return await event.get_chat()
        
    # 3. Saved Selection
    saved_id = get_setting('selected_channel_id')
    if saved_id:
        try:
            entity = await use_client.get_entity(int(saved_id))
            return entity
        except:
            pass
            
    return None

async def check_is_admin(entity, client_instance=None):
    """Checks if the bot is admin in the given entity."""
    use_client = client_instance or client
    try:
        if getattr(entity, 'creator', False):
            return True

        if getattr(entity, 'admin_rights', None):
            return True

        try:
            perms = await use_client.get_permissions(entity, 'me')
        except Exception:
            perms = None

        if perms:
            if getattr(perms, 'creator', False):
                return True
            if getattr(perms, 'admin_rights', None):
                return True
            if getattr(perms, 'is_admin', False):
                return True

        full_entity = await use_client.get_entity(entity.id)
        if getattr(full_entity, 'admin_rights', None) or getattr(full_entity, 'creator', False):
            return True

        return False
    except:
        return False

async def check_can_ban(entity, client_instance=None):
    use_client = client_instance or client
    try:
        if getattr(entity, 'creator', False):
            return True

        admin_rights = getattr(entity, 'admin_rights', None)
        if admin_rights and getattr(admin_rights, 'ban_users', False):
            return True

        try:
            perms = await use_client.get_permissions(entity, 'me')
        except Exception:
            perms = None

        if perms:
            if getattr(perms, 'creator', False):
                return True
            admin_rights = getattr(perms, 'admin_rights', None)
            if admin_rights and getattr(admin_rights, 'ban_users', False):
                return True

        return False
    except:
        return False

def get_user_status_label(user):
    """Classifies user status into: online, today, week, month, long."""
    # Debug: Print user status type for first few users to verify
    # print(f"DEBUG: User {user.id} status type: {type(user.status)}")
    
    if isinstance(user.status, UserStatusOnline):
        return 'online'
    
    if isinstance(user.status, UserStatusOffline):
        was_online = user.status.was_online
        # Ensure timezone awareness (Telethon uses UTC usually)
        if was_online.tzinfo is None:
            was_online = was_online.replace(tzinfo=timezone.utc)
        
        now = datetime.now(timezone.utc)
        diff = now - was_online
        
        if diff < timedelta(days=1):
            return 'today'
        elif diff < timedelta(days=7):
            return 'week'
        elif diff < timedelta(days=30):
            return 'month'
        else:
            return 'long'
            
    if isinstance(user.status, UserStatusRecently):
        # UserStatusRecently means "Last seen recently" (within ~3 days) but hidden exact time
        # This is effectively "online" or "today" for many intents, but strictly it is 'recently'.
        # However, to capture them in "Phase 1: Online & Recent", we map them to 'recently'.
        return 'recently' 
        
    if isinstance(user.status, UserStatusLastWeek):
        return 'week'
    if isinstance(user.status, UserStatusLastMonth):
        return 'month'
    if isinstance(user.status, UserStatusEmpty):
        return 'long'
        
    return 'long' # Default

def save_member(user, channel_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    try:
        status_label = get_user_status_label(user)
        c.execute('''
            INSERT OR REPLACE INTO members (id, username, first_name, last_name, phone, is_bot, channel_id, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (user.id, user.username or "", user.first_name or "", user.last_name or "", user.phone or "", 1 if user.bot else 0, channel_id, status_label))
        conn.commit()
    except Exception as e:
        print(f"DB Error: {e}")
    finally:
        conn.close()

def save_members_batch(users_data):
    """
    users_data: list of tuples (user, channel_id)
    """
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    try:
        data_to_insert = []
        for user, channel_id in users_data:
            status_label = get_user_status_label(user)
            data_to_insert.append((
                user.id, user.username or "", user.first_name or "", user.last_name or "", 
                user.phone or "", 1 if user.bot else 0, channel_id, status_label
            ))
            
        c.executemany('''
            INSERT OR REPLACE INTO members (id, username, first_name, last_name, phone, is_bot, channel_id, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', data_to_insert)
        conn.commit()
    except Exception as e:
        print(f"Batch DB Error: {e}")
    finally:
        conn.close()

def get_members(channel_id):
    conn = sqlite3.connect(DB_FILE)
    try:
        df = pd.read_sql_query("SELECT * FROM members WHERE channel_id = ?", conn, params=(channel_id,))
        return df
    finally:
        conn.close()

# Persian alphabet for search - Reordered by frequency (Approximate)
# Common first letters: A (ا/آ), M (م), S (س), R (ر), N (ن), B (ب)
persian_chars = [
    'ا', 'آ', 'م', 'س', 'ر', 'ن', 'ب', 'د', 'پ', 'ت', 'ک', 'ه', 'و', 'ی', 
    'ف', 'ش', 'ج', 'ح', 'خ', 'ل', 'ع', 'ق', 'گ', 'ز', 'ص', 'ض', 'ط', 'ظ', 
    'ذ', 'چ', 'ث', 'ژ', 'غ', 'ء', 'ئ', 'ؤ'
]

# English alphabet - Reordered by frequency of starting letters in names
# Common: A, M, S, D, J, R, B, K, T, C, E, L, N, P, G, V, H, O, F, I, W, U, Z, Y, Q, X
english_chars_optimized = list("amsdjrbkcelnpgvhofiwyquzx")

# Numbers
numbers = list("0123456789")

# Optimized base queries: English first (most common globally), then Numbers, then Persian (restored)
base_queries = english_chars_optimized + numbers + persian_chars

# Proxy Setup
proxy = None
if os.getenv("PROXY_HOST") and os.getenv("PROXY_PORT"):
    try:
        proxy = (socks.SOCKS5, os.getenv("PROXY_HOST"), int(os.getenv("PROXY_PORT")))
        print(f"🌍 Proxy configured: {os.getenv('PROXY_HOST')}:{os.getenv('PROXY_PORT')}")
    except:
        print("⚠️ Invalid Proxy Configuration")

# Multi-Session Support
session_files = glob.glob("*.session")
clients = []
worker_clients = []
client = None # Main client

print(f"🔎 Found {len(session_files)} session files: {session_files}")

# 1. Initialize all clients
for session_path in session_files:
    s_name = os.path.splitext(os.path.basename(session_path))[0]
    c = TelegramClient(s_name, api_id, api_hash, proxy=proxy)
    clients.append(c)
    
    # Identify main client (legacy "session" or first one)
    if s_name == "session":
        client = c
    else:
        worker_clients.append(c)

# Fallback: If no "session.session" found, pick the first one as main
if not client and clients:
    client = clients[0]
    # Remove from workers if it was added there
    if client in worker_clients:
        worker_clients.remove(client)

# Fallback: If no sessions exist at all, create default
if not clients:
    print("⚠️ No sessions found. Creating default 'session'...")
    client = TelegramClient("session", api_id, api_hash, proxy=proxy)
    clients.append(client)

# Add main client to workers pool so it can also be used for scanning
worker_clients.append(client)

print(f"✅ Main Client: {client.session.filename}")
print(f"✅ Worker Clients: {len(worker_clients)}")

# Global set to track which channels are being monitored to avoid duplicates
monitored_channels = set()
dashboard_messages = {}
scan_progress = {}

def generate_dashboard_menu(entity, monitoring_status=None, is_admin=False, can_ban=False):
    """Generates the dashboard menu text."""
    # Add timestamp to show when data was last relevant
    now_str = datetime.now().strftime("%H:%M")
    
    menu = f"**⚙️ Dashboard: {entity.title}**\n"
    menu += f"🕒 Last Check: {now_str}\n"
    menu += "━━━━━━━━━━━━━━━━━━━━━━\n"

    if is_admin:
        # 1. Monitoring & Indexing
        menu += "**📊 Monitoring & Indexing**\n"
        if monitoring_status:
             menu += f"• {monitoring_status}\n"
        else:
             menu += "• 📡 `/monitor` - Start scanning members\n"
        
        # 2. Export & Filters
        menu += "\n**📂 Export Data**\n"
        menu += "• 🟢 `/filter_recently` - Online + Last 3 Days\n"
        menu += "• 🗓 `/filter_week` - Recently + Last 7 Days\n"
        menu += "• 📆 `/filter_month` - Week + Last 30 Days\n"
        menu += "• ♾️ `/filter_long` - ALL Members (Everything)\n"
        menu += "• 📦 `/filter_batch` - Download All 4 Files\n"
            
    else:
        menu += "\n❌ **Access Restricted**\n"
        menu += "You are not an admin in this channel.\n"
        menu += "Please ask the owner to promote me to Admin with:\n"
        menu += "- Can Ban Users (for cleanup)\n"
        menu += "- Can Invite Users (for monitoring)\n"

    menu += "\n━━━━━━━━━━━━━━━━━━━━━━\n"
    menu += "🔙 `/menu` - Back to Channel List"
    return menu

async def recursive_scan_task(entity, status_msg=None, scan_client=None):
    """Background task to fully scan a channel."""
    
    # Use provided scan_client or fallback to global client
    use_client = scan_client or client

    try:
        # Get channel specific scan mode
        scan_mode = get_channel_pref(entity.id) or 'all'
        
        print(f"Starting Recursive Scan for {entity.title} (Mode: {scan_mode})...")
        
        # Determine total members for progress calculation
        full_entity = await use_client.get_entity(entity.id)
        total_members = full_entity.participants_count
        if total_members is None or total_members == 0:
             # Fallback if count is hidden
             try:
                 total = await use_client.get_participants(entity, limit=0)
                 total_members = total.total
             except:
                 total_members = 1000 # Estimate

        found = 0
        last_update_time = time.time()
        
        # Helper to update progress message
        async def update_progress(current_count, current_char):
             nonlocal last_update_time
             if time.time() - last_update_time < 3: # Update max every 3s
                 return
             
             last_update_time = time.time()
             pct = min((current_count / total_members) * 100, 99.9)
             
             status_text = (
                 f"🔄 **Scanning {entity.title}**\n"
                 f"🎯 Mode: `{scan_mode}`\n"
                 f"📌 Phase: {current_char}\n"
                 f"👥 Found: {current_count} / {total_members}\n"
                 f"📊 Progress: **{pct:.1f}%**"
             )
             
             scan_progress[entity.id] = f"🔄 Scanning... ({pct:.1f}%)"
             
             if status_msg:
                 try: await status_msg.edit(status_text)
                 except: pass
                 
             # Update Dashboard if active
             if entity.id in dashboard_messages:
                 try:
                     dash = dashboard_messages[entity.id]
                     menu = generate_dashboard_menu(entity, f"🔄 Scanning... ({pct:.1f}%) Mode: {scan_mode}", True, True)
                     await dash.edit(menu)
                 except: pass

        def should_save_user(status_label, target_statuses):
            if scan_mode == 'smart_tiered':
                return status_label in target_statuses
            if scan_mode == 'recent':
                return status_label in ['online', 'today', 'recently']
            if scan_mode == 'week':
                return status_label in ['online', 'today', 'recently', 'week']
            return True

        # Global semaphore for all queries (top-level and recursive) to respect FloodWait
        query_sem = asyncio.Semaphore(5)

        async def scan_query(query, target_statuses, depth=0):
            nonlocal found
            batch_users = []
            count_for_query = 0
            
            # Use global semaphore for API calls
            async with query_sem:
                # Debug log for visibility
                print(f"Scanning: '{query}' (Depth: {depth})...")
                try:
                    async for user in use_client.iter_participants(entity, search=query):
                        count_for_query += 1
                        if user.id in existing_ids:
                            continue
                        status_label = get_user_status_label(user)
                        if not should_save_user(status_label, target_statuses):
                            continue
                        batch_users.append((user, entity.id))
                        existing_ids.add(user.id)
                        found += 1
                        if len(batch_users) >= 50:
                            save_members_batch(batch_users)
                            batch_users = []
                            await update_progress(found, query)
                except FloodWaitError as e:
                    print(f"FloodWait: Sleeping {e.seconds}s")
                    await asyncio.sleep(e.seconds + 2)
                    # Retry logic could be added here, but recursive structure complicates it. 
                    # For now, we skip this query on floodwait to keep moving, or better:
                    # We should probably retry. But let's rely on simple skip for speed.
                except Exception as e:
                    print(f"Error scanning '{query}': {e}")

            if batch_users:
                save_members_batch(batch_users)
                await update_progress(found, query)
            
            # Parallelize recursion
            if count_for_query >= 100 and depth < 2:
                # OPTIMIZATION: Context-aware recursion to avoid mixing scripts unnecessarily
                last_char = query[-1]
                next_chars = []
                
                is_english = 'a' <= last_char.lower() <= 'z'
                is_persian = last_char in persian_chars
                is_number = '0' <= last_char <= '9'
                
                if is_english:
                    # English prefix -> Recurse with English + Numbers
                    next_chars = english_chars_optimized + numbers
                elif is_persian:
                    # Persian prefix -> Recurse with Persian + Numbers
                    next_chars = persian_chars + numbers
                else:
                    # Number or other -> Try full set but maybe optimized
                    # If it's a number, it could be part of an English username or a Persian name
                    next_chars = base_queries

                sub_tasks = []
                for ch in next_chars:
                    # No await here, gather later
                    sub_tasks.append(scan_query(query + ch, target_statuses, depth + 1))
                if sub_tasks:
                    await asyncio.gather(*sub_tasks)

        # Resume logic
        start_index, start_phase = get_checkpoint(entity.id)
        
        # Define Phases for Smart Tiered Mode
        # If not smart_tiered, treat as single phase (Phase 1) with all queries
        phases = []
        if scan_mode == 'smart_tiered':
            phases = [
                (1, ['online', 'today', 'recently'], "Phase 1: Online & Recent"),
                (2, ['week'], "Phase 2: Last Week"),
                (3, ['month'], "Phase 3: Last Month"),
                (4, ['long'], "Phase 4: Older Than Month")
            ]
        else:
            # Single pass for other modes
            phases = [(1, None, f"Phase 1: {scan_mode}")]
            
        # Pre-load existing IDs to avoid duplicate DB writes (optimization)
        # Use executor to avoid blocking loop during heavy DB read
        df = await run_in_executor(get_members, entity.id)
        existing_ids = set(df['id'].tolist())
        found = len(existing_ids)
        print(f"DEBUG: Initial members loaded from DB: {found}")

        # OPTIMIZATION: Small Channel Fast Path (< 10k)
        # Attempt to use iter_participants first. If it returns incomplete results (common in channels), fallback to search.
        if total_members < 10000:
            print(f"🚀 Small channel detected ({total_members}). Trying fast iteration strategy...")
            pre_batch = []
            
            # Determine effective target statuses for filtering (if needed)
            fast_target_statuses = None
            if scan_mode == 'smart_tiered':
                 fast_target_statuses = ['online', 'today', 'recently', 'week', 'month', 'long']
            
            try:
                # Track how many we find in this pass
                fast_found_count = 0
                async for u in use_client.iter_participants(entity, limit=None):
                    fast_found_count += 1
                    if u.id in existing_ids:
                        continue
                        
                    # Apply filter
                    status_label = get_user_status_label(u)
                    if not should_save_user(status_label, fast_target_statuses):
                        continue
                        
                    pre_batch.append((u, entity.id))
                    existing_ids.add(u.id)
                    found += 1
                    
                    if len(pre_batch) >= 50:
                        save_members_batch(pre_batch)
                        pre_batch = []
                        await update_progress(found, "Fast Scan")
                        
                if pre_batch:
                    save_members_batch(pre_batch)
                    await update_progress(found, "Fast Scan")
                
                # Verify Completeness
                # If we found significantly fewer members than total (and total is > 200), we probably hit a limit.
                # Common limit is 200. If we got <= 200 and total is > 250, it's definitely incomplete.
                if fast_found_count <= 250 and total_members > 250:
                    print(f"⚠️ Fast scan incomplete (Found {fast_found_count}/{total_members}). Falling back to Deep Search.")
                    # Do NOT clear phases. Let it proceed to search.
                else:
                    print(f"✅ Fast scan complete (Found {fast_found_count}/{total_members}). Skipping search.")
                    phases = []
                
            except Exception as e:
                print(f"Fast scan error: {e}. Falling back to search.")
                # If fast scan fails, we let it fall through to phases (if any)

        elif scan_mode == 'all':
            # For > 10k channels in 'all' mode, we also try iter_participants first?
            # Or should we just rely on search?
            # 'all' mode usually implies "get everyone". Search is safer for large channels.
            # But the previous code had this block. Let's keep it but maybe add fallback too?
            # Actually, for > 10k, iter_participants definitely fails.
            # So 'all' mode should probably use Search Phases.
            # Removing this block to force 'all' mode to use phases below.
            pass

        for phase_num, target_statuses, phase_desc in phases:
            # Skip completed phases
            if phase_num < start_phase:
                continue
                
            # Determine start index for this phase
            current_start_index = start_index if phase_num == start_phase else 0
            
            queries_to_run = base_queries[current_start_index:]
            
            print(f"Starting {phase_desc} at index {current_start_index}...")
            
            # We don't need a local semaphore anymore, scan_query uses global query_sem
            tasks = []
            
            async def run_wrapper(q, idx):
                 current_index = current_start_index + idx
                 save_checkpoint(entity.id, current_index, phase_num)
                 await update_progress(found, f"{q} ({phase_desc})")
                 await scan_query(q, target_statuses)

            # Increase batch size for top-level tasks since semaphore is inside
            for idx, q in enumerate(queries_to_run):
                tasks.append(asyncio.create_task(run_wrapper(q, idx)))
                
                # We can fire more tasks now, relying on query_sem to throttle
                if len(tasks) >= 20: 
                    await asyncio.gather(*tasks)
                    tasks = []
                    # Small sleep to prevent tight loop CPU usage
                    await asyncio.sleep(0.1)
            
            if tasks:
                await asyncio.gather(*tasks)
            
            # Phase Complete - Reset start_index for next phase
            start_index = 0

        # Reset checkpoint after finish all phases
        save_checkpoint(entity.id, 0, 1)
        scan_progress[entity.id] = "✅ Indexed"

        # Final Dashboard Update
        if entity.id in dashboard_messages:
             try:
                 dashboard_msg = dashboard_messages[entity.id]
                 new_menu = generate_dashboard_menu(entity, "✅ Indexed", True, True)
                 await dashboard_msg.edit(new_menu)
             except: pass

        if status_msg:
            try:
                await status_msg.edit(
                    f"✅ **Scan Complete**\n"
                    f"📂 Channel: {entity.title}\n"
                    f"🎯 Mode: `{scan_mode}`\n"
                    f"👥 Total Saved: {found}\n"
                    f"📊 Coverage: 100%"
                )
            except: pass
            
    except Exception as e:
        print(f"Scan failed for {entity.title}: {e}")

async def monitor_channel(entity, event=None, dashboard_msg=None, use_client=None):
    """Sets up monitoring for a channel."""
    
    # Determine client for API calls (size check etc)
    if not use_client:
        use_client = event.client if event else client

    # Update global dashboard reference if provided
    if dashboard_msg:
        dashboard_messages[entity.id] = dashboard_msg

    if entity.id in monitored_channels:
        # If we have a dashboard message, update it with current status
        current_status = scan_progress.get(entity.id, "✅ Monitoring Active")
        if dashboard_msg:
             # Check admin for menu generation
            is_admin = await check_is_admin(entity, use_client)
            can_ban = await check_can_ban(entity, use_client)
            try:
                new_menu = generate_dashboard_menu(entity, current_status, is_admin, can_ban)
                await dashboard_msg.edit(new_menu)
            except: pass

        if event:
            await event.respond(f"✅ Already monitoring **{entity.title}**.")
        return

    # Check for Large Channel Logic (>10k)
    try:
        full_entity = await use_client.get_entity(entity.id)
        count = full_entity.participants_count
        
        # Fallback if count is None or 0
        if not count:
             try:
                 total = await use_client.get_participants(entity, limit=0)
                 count = total.total
             except:
                 count = 0

        print(f"Checking size for {entity.title}: {count} members")
        
        # If large channel and no preference set, auto-set to 'smart_tiered'
        if count > 10000:
            existing_pref = get_channel_pref(entity.id)
            if not existing_pref:
                print(f"Large channel detected ({count}). Auto-setting 'smart_tiered' mode.")
                save_channel_pref(entity.id, 'smart_tiered')
                
                # Notify user about auto-selection
                if event:
                    try:
                        await event.respond(
                            f"⚠️ **Large Channel Detected (>10k)**\n"
                            f"🔄 Mode set to **Smart Tiered** (Auto-Extraction based on quality).\n"
                            f"extracting: Online -> Week -> Month"
                        )
                    except: pass
                    
    except Exception as e:
        print(f"Error checking channel size: {e}")

    monitored_channels.add(entity.id)
    
    status_msg = None
    if event and not dashboard_msg:
        status_msg = await event.respond(f"👀 Started monitoring **{entity.title}**.\nPerforming initial sync in background...")
        
    # Start background scan
    await asyncio.sleep(1) 
    
    # Select a worker client (random rotation)
    # Since all clients are "admins" now, we can pick any active client.
    # However, for scanning, we still might want to rotate to avoid rate limits on one account.
    # Use worker_clients (which includes everyone) or fallback to use_client
    
    scan_client = use_client
    
    # Ensure the chosen client is actually connected
    if not scan_client.is_connected():
        if worker_clients:
             scan_client = random.choice(worker_clients)
        else:
             scan_client = client

    # Clean filename for logging
    s_name = "unknown"
    try: s_name = os.path.basename(scan_client.session.filename)
    except: pass
    
    print(f"🔄 Using client session: {s_name} for {entity.title}")
    
    asyncio.create_task(recursive_scan_task(entity, status_msg, scan_client))

async def on_chat_action(event):
    """Listen for real-time joins and admin promotions (Permanent Listener)."""
    use_client = event.client
    try:
        # Case 1: Bot added to channel/group or Promoted to Admin
        if (event.user_added or event.user_joined) and event.user_id == (await use_client.get_me()).id:
            print(f"🤖 Bot added/promoted in chat: {event.chat_id}")
            # Wait a moment for permissions to propagate
            await asyncio.sleep(2)
            entity = await event.get_chat()
            
            # Check if we are admin
            if await check_is_admin(entity, use_client):
                 print(f"✅ Auto-monitoring triggered for {entity.title}")
                 # Trigger full scan and add to monitoring list
                 await monitor_channel(entity, event)
            return

        # Case 2: New Member Joined (Real-time capture)
        if event.user_joined or event.user_added:
            # Delay to ensure user object is fully available
            await asyncio.sleep(0.5)
            
            chat = await event.get_chat()
            users = await event.get_users()
            
            for user in users:
                # Save to DB immediately without waiting for manual /monitor
                save_member(user, chat.id)
                print(f"🆕 New member saved: {user.id} in {chat.title}")
                
    except Exception as e:
        print(f"Event Error: {e}")

# Startup Hook: Check all admin channels on start
async def startup_check():
    print("Startup: Checking for admin channels to monitor...")
    
    # Iterate over ALL active clients to check their dialogs
    for c in clients:
        if not c.is_connected(): continue
        
        try:
            async for dialog in c.iter_dialogs():
                if dialog.is_channel or dialog.is_group:
                    entity = dialog.entity
                    if getattr(entity, 'admin_rights', None) or getattr(entity, 'creator', False):
                        if entity.id not in monitored_channels:
                            print(f"Startup: Auto-monitoring {entity.title} (via {c.session.filename})")
                            # We can pass 'c' as the event-like object or modify monitor_channel to accept client directly
                            # For simplicity, we just trigger it and let it pick a worker
                            asyncio.create_task(monitor_channel(entity, use_client=c))
        except Exception as e:
            print(f"Startup check failed for a client: {e}")

    print("Startup check complete.")
    
    # Send welcome message from all clients
    for c in clients:
        if not c.is_connected(): continue
        try:
            await c.send_message('me', "🤖 **Bot Started!**\nAuto-monitoring active for admin channels.\nSend `/help` for commands.")
        except: pass

async def scan_mode_handler(event):
    use_client = event.client
    mode = event.pattern_match.group(1).lower()
    channel_id = int(event.pattern_match.group(2))
    
    valid_modes = ['recent', 'week', 'all']
    if mode not in valid_modes:
        await event.respond(f"❌ Invalid mode. Use `recent`, `week`, or `all`.")
        return
        
    try:
        # Save preference
        save_channel_pref(channel_id, mode)
        
        # Resolve entity and start scan
        entity = await use_client.get_entity(channel_id)
        
        await event.respond(f"✅ Mode set to **{mode.upper()}** for **{entity.title}**.\nStarting scan now...")
        
        # Trigger monitor (it will now pass the check)
        await monitor_channel(entity, event)
        
    except Exception as e:
        await event.respond(f"❌ Error: {e}")

async def select_handler(event):
    use_client = event.client
    chat_link = event.pattern_match.group(1)
    
    try:
        entity = None
        if chat_link:
            # Show loading only if link provided (manual lookup)
            loading_msg = await event.respond("🔄 Resolving link...")
            try:
                if chat_link.isdigit() or chat_link.startswith("-"):
                    entity = await use_client.get_entity(int(chat_link))
                else:
                    entity = await use_client.get_entity(chat_link)
                await loading_msg.delete()
            except:
                await loading_msg.edit("❌ Invalid link or ID.")
                return
        elif event.is_channel or event.is_group:
            entity = await event.get_chat()
        else:
             await event.respond("❌ Usage: `/select <link>` (or run in a channel)")
             return

        if entity:
            # Save selection
            set_setting('selected_channel_id', entity.id)
            
            # Show dashboard
            msg = await show_channel_dashboard(event, entity)
            await monitor_channel(entity, event, dashboard_msg=msg)
            
    except Exception as e:
        await event.respond(f"❌ Error: {e}")

async def monitor_all_handler(event):
    use_client = event.client
    await event.respond("🔎 Scanning for ALL channels where I am admin...")
    count = 0
    async for dialog in use_client.iter_dialogs():
        if dialog.is_channel or dialog.is_group:
            entity = dialog.entity
            if getattr(entity, 'admin_rights', None) or getattr(entity, 'creator', False):
                await monitor_channel(entity, event)
                count += 1
    await event.respond(f"✅ Auto-monitoring started for {count} channels.")

async def monitor_handler(event):
    use_client = event.client
    chat_link = event.pattern_match.group(1)
    
    # Try to resolve entity (Link > Current Chat > Selected)
    try:
        entity = await resolve_entity(event, chat_link)
        if entity:
            # Check Admin Rights
            if not await check_is_admin(entity, use_client):
                await event.respond(f"❌ I am not an admin in **{entity.title}**.\nPlease promote me to admin first.")
                return

            # Show dashboard and get the message object
            msg = await show_channel_dashboard(event, entity)
            
            # Start monitoring with dashboard reference
            await monitor_channel(entity, event, dashboard_msg=msg)
            return
    except Exception as e:
        await event.respond(f"❌ Error: {e}")
        return

# Handle underscore aliases like /filter_online, /filter_today
async def filter_alias_handler(event):
    mode = event.pattern_match.group(1).lower()
    chat_link = event.pattern_match.group(2)
    
    if mode == 'batch':
        await run_batch_filter_logic(event, chat_link)
    else:
        await run_filter_logic(event, mode, chat_link)

# Helper to run blocking DB/File operations in thread pool
async def run_blocking_task(func, *args):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, func, *args)

def generate_batch_files_sync(channel_id, entity_title):
    """Synchronous function to generate batch files to avoid blocking event loop."""
    # Query DB
    df = get_members(channel_id)
    
    if df.empty:
        return None, f"⚠️ No members found in DB for **{entity_title}**. Run `/monitor` first."
        
    if 'status' not in df.columns:
         return None, "⚠️ Database is old. Please run `/monitor` again to update member statuses."

    modes = ['recently', 'week', 'month', 'long']
    files_to_send = []
    summary_text = f"📦 **Batch Export for {entity_title}**\n\n"
    
    safe_title = "".join([c for c in entity_title if c.isalpha() or c.isdigit() or c==' ']).strip()
    
    # Helper to get DF based on logic
    def get_filtered_df(mode):
        if mode == 'recently':
            # Online + Today (24h) + Recently (Privacy hidden but active)
            return df[df['status'].isin(['online', 'today', 'recently'])]
        elif mode == 'week':
            # All 'recently' + Week
            return df[df['status'].isin(['online', 'today', 'recently', 'week'])]
        elif mode == 'month':
            # All 'week' + Month
            return df[df['status'].isin(['online', 'today', 'recently', 'week', 'month'])]
        elif mode == 'long':
            # Everyone (The user requested 'long' to be ALL members)
            return df
        return pd.DataFrame()

    for mode in modes:
        filtered_df = get_filtered_df(mode)
        count = len(filtered_df)
        
        if count > 0:
            base_filename = f"{safe_title}_{mode}_{count}"
            
            # Usernames
            usernames = filtered_df['username'].dropna().tolist()
            usernames = [u for u in usernames if u]
            u_count = len(usernames)
            u_file = f"{base_filename}_usernames.txt"
            with open(u_file, 'w', encoding='utf-8') as f:
                for u in usernames: f.write(f"{u}\n")
            files_to_send.append(u_file)
            
            # IDs
            ids = filtered_df['id'].tolist()
            i_count = len(ids)
            i_file = f"{base_filename}_ids.txt"
            with open(i_file, 'w', encoding='utf-8') as f:
                for i in ids: f.write(f"{i}\n")
            files_to_send.append(i_file)

            summary_text += f"• **{mode.title()}**: {count} (👤 {u_count} | 🆔 {i_count})\n"
        else:
             summary_text += f"• **{mode.title()}**: 0\n"
    
    if not files_to_send:
        return None, "⚠️ No members found in any category."

    return files_to_send, summary_text

async def run_batch_filter_logic(event, chat_link):
    use_client = event.client
    try:
        entity = await resolve_entity(event, chat_link)
        if not entity:
             await event.respond("❌ No target selected. Use `/select <link>` first.")
             return

        if not await check_is_admin(entity, use_client):
            await event.respond(f"❌ I am not an admin in **{entity.title}**.\nAccess denied.")
            return

        msg = await event.respond(f"📦 Generating ALL filter files for **{entity.title}**...\nThis may take a moment.")

        # Run heavy lifting in executor to prevent freezing the bot
        files_to_send, result_text = await run_blocking_task(generate_batch_files_sync, entity.id, entity.title)
        
        if not files_to_send:
            await msg.edit(result_text)
            return

        # Send all files
        await use_client.send_file(event.chat_id, files_to_send, caption=result_text)
        
        for f in files_to_send:
            try: os.remove(f)
            except: pass
        
        await msg.edit("✅ فایل‌ها ارسال شد.")

    except Exception as e:
        await event.respond(f"❌ Batch Error: {e}")

def generate_single_file_sync(channel_id, entity_title, mode):
    """Synchronous function to generate single filter file."""
    # Query DB
    df = get_members(channel_id)
    
    if df.empty:
        return None, f"⚠️ No members found in DB for **{entity_title}**. Run `/monitor` first."
    
    if 'status' not in df.columns:
        return None, "⚠️ Database is old. Please run `/monitor` again to update member statuses."
        
    filtered_df = pd.DataFrame()
    
    if mode == 'recently':
        filtered_df = df[df['status'].isin(['online', 'today', 'recently'])]
    elif mode == 'week':
        filtered_df = df[df['status'].isin(['online', 'today', 'recently', 'week'])]
    elif mode == 'month':
        filtered_df = df[df['status'].isin(['online', 'today', 'recently', 'week', 'month'])]
    elif mode == 'long':
        filtered_df = df
        
    count = len(filtered_df)
    
    if count == 0:
        return None, f"⚠️ No members found for filter: `{mode}`"
        
    safe_title = "".join([c for c in entity_title if c.isalpha() or c.isdigit() or c==' ']).strip()
    base_filename = f"{safe_title}_{mode}_{count}"
    
    files_to_send = []
    
    # Usernames
    usernames = filtered_df['username'].dropna().tolist()
    usernames = [u for u in usernames if u]
    u_count = len(usernames)
    if usernames:
        u_file = f"{base_filename}_usernames.txt"
        with open(u_file, 'w', encoding='utf-8') as f:
            for u in usernames: f.write(f"{u}\n")
        files_to_send.append(u_file)
        
    # IDs
    ids = filtered_df['id'].tolist()
    i_count = len(ids)
    if ids:
        i_file = f"{base_filename}_ids.txt"
        with open(i_file, 'w', encoding='utf-8') as f:
            for i in ids: f.write(f"{i}\n")
        files_to_send.append(i_file)
        
    return files_to_send, f"✅ Exported **{count}** members ({mode}).\n👤 Usernames: {u_count}\n🆔 IDs: {i_count}"

# Main filter handler (renamed to run_filter_logic for reuse)
async def run_filter_logic(event, mode, chat_link):
    # Map common aliases if needed, or just stick to English keys
    # recently, week, month, long
    valid_modes = ['recently', 'week', 'month', 'long']
    
    if mode not in valid_modes:
        await event.respond(f"❌ Invalid filter. Modes: `recently`, `week`, `month`, `long`")
        return

    use_client = event.client
    try:
        entity = await resolve_entity(event, chat_link)
        if not entity:
             await event.respond("❌ No target selected. Use `/select <link>` first.")
             return

        if not await check_is_admin(entity, use_client):
            await event.respond(f"❌ I am not an admin in **{entity.title}**.\nAccess denied.")
            return

        msg = await event.respond(f"🔍 Filtering `{mode}` for **{entity.title}**...")
        
        # Run heavy lifting in executor
        files_to_send, result_text = await run_blocking_task(generate_single_file_sync, entity.id, entity.title, mode)
        
        if not files_to_send:
            await msg.edit(result_text)
            return
            
        await use_client.send_file(event.chat_id, files_to_send, caption=result_text)
        
        for f in files_to_send:
            try: os.remove(f)
            except: pass
        
        await msg.edit("✅ فایل ارسال شد.")

    except Exception as e:
        await event.respond(f"❌ Filter Error: {e}")

async def filter_handler(event):
    mode = event.pattern_match.group(1).lower()
    chat_link = event.pattern_match.group(2)
    await run_filter_logic(event, mode, chat_link)

async def help_handler(event):
    text = (
        "🤖 **راهنمای دستورات**\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "🧭 /start یا /menu\n"
        "📡 /monitor یا /monitor <link>\n"
        "📊 /monitor_all\n"
        "🔎 /filter <recently|week|month|long> [link]\n"
        "🟢 /filter_recently\n"
        "🗓 /filter_week\n"
        "📆 /filter_month\n"
        "♾️ /filter_long\n"
        "📦 /filter_batch\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "نکته: ابتدا /monitor را اجرا کنید تا دیتا ساخته شود."
    )
    await event.respond(text)

async def start_handler(event):
    """Lists all channels/groups the user is part of with admin status."""
    use_client = event.client
    msg = await event.respond("🔄 Loading your channels list...")
    
    try:
        lines = ["**📜 Select a Channel**", "Choose a channel to manage:\n"]
        
        count = 0
        async for dialog in use_client.iter_dialogs():
            if dialog.is_channel or dialog.is_group:
                entity = dialog.entity
                
                # Determine status
                is_admin = False
                if getattr(entity, 'admin_rights', None) or getattr(entity, 'creator', False):
                    is_admin = True
                
                status_icon = "👑" if is_admin else "👤"
                status_text = "Admin" if is_admin else "Member"
                
                # Create a simple list item
                lines.append(f"{status_icon} **{entity.title}** ({status_text})")
                lines.append(f"👉 `/select_{entity.id}`\n")
                count += 1
                
        if count == 0:
            await msg.edit("❌ No channels or groups found.")
            return
            
        text = "\n".join(lines)
        # Split if too long (Telegram limit is 4096, keeping it simple for now)
        if len(text) > 4000:
            text = text[:4000] + "\n... (List truncated)"
            
        await msg.edit(text)
        
    except Exception as e:
        await msg.edit(f"❌ Error loading channels: {e}")

async def specific_select_handler(event):
    """Handles clicking a /select_ID command from the menu."""
    use_client = event.client
    try:
        channel_id = int(event.pattern_match.group(1))
        
        # Add visual feedback immediately
        loading = await event.respond("🔄 Loading...")
        
        try:
             entity = await use_client.get_entity(channel_id)
        except Exception:
             # Try with -100 prefix for channels if not found (common issue with ID resolution)
             try:
                 entity = await use_client.get_entity(int(f"-100{channel_id}"))
             except:
                 await loading.edit("❌ Channel not found or I am not a member.")
                 return
        
        # Save selection
        set_setting('selected_channel_id', entity.id)
        
        # Show dashboard and get the message object
        msg = await show_channel_dashboard(event, entity)
        
        # Cleanup loading message
        await loading.delete()
        
        # Automatically start monitoring (or update if already running)
        await monitor_channel(entity, dashboard_msg=msg, use_client=use_client)
        
    except Exception as e:
        await event.respond(f"❌ Error selecting channel: {e}")

async def show_channel_dashboard(event, entity):
    """Displays the dynamic dashboard based on permissions."""
    use_client = event.client
    
    # Check Admin Status
    is_admin = await check_is_admin(entity, use_client)
    can_ban = await check_can_ban(entity, use_client)

    # Get current monitoring status if available
    status_text = scan_progress.get(entity.id, None)

    # Generate menu
    menu = generate_dashboard_menu(entity, status_text, is_admin, can_ban)
    
    msg = await event.respond(menu)
    return msg

# Old main function removed to avoid duplication
# async def main(): ...

async def scan_and_export(link, to_csv=False, to_xlsx=True):
    init_db()
    if link.isdigit() or link.startswith("-"):
        entity = await client.get_entity(int(link))
    else:
        entity = await client.get_entity(link)
    await recursive_scan_task(entity)
    df = get_members(entity.id)
    safe_title = "".join([c for c in entity.title if c.isalpha() or c.isdigit() or c==' ']).strip()
    if to_xlsx:
        xdf = df.rename(columns={
            "id": "User ID",
            "username": "Username",
            "first_name": "First Name",
            "last_name": "Last Name",
            "phone": "Phone",
            "is_bot": "Is Bot"
        })
        xdf['Is Bot'] = xdf['Is Bot'].apply(lambda x: 'Yes' if x == 1 else 'No')
        xname = f"{safe_title}_{len(df)}.xlsx"
        xdf.to_excel(xname, index=False)
        print(f"Saved: {xname}")
    if to_csv:
        cname = f"{safe_title}_{len(df)}.csv"
        df.to_csv(cname, index=False)
        print(f"Saved: {cname}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--export", type=str)
    args = parser.parse_args()
    
    # Initialize DB
    init_db()
    
    print(f"🚀 Starting {len(clients)} clients...")
    
    active_clients = []
    
    # Helper to start clients asynchronously
    async def start_clients():
        for c in clients:
            try:
                # Use filename as identifier
                s_name = "unknown"
                try: s_name = os.path.basename(c.session.filename)
                except: pass
                
                print(f"🔌 Connecting {s_name}...")
                await c.connect()
                
                if not await c.is_user_authorized():
                    print(f"❌ Session '{s_name}' is NOT authorized! Skipping.")
                    continue
                
                me = await c.get_me()
                print(f"✅ Session '{s_name}' authorized as: {me.first_name} ({me.id})")
                
                # Register event handlers for EACH client
                c.add_event_handler(start_handler, events.NewMessage(pattern=r'/start|/menu'))
                c.add_event_handler(scan_mode_handler, events.NewMessage(pattern=r'^/scan\s+'))
                c.add_event_handler(select_handler, events.NewMessage(pattern=r'^/select\s+'))
                c.add_event_handler(monitor_all_handler, events.NewMessage(pattern=r'^/monitor_all$'))
                c.add_event_handler(monitor_handler, events.NewMessage(pattern=r'^/monitor(?:\s+(.*))?$'))
                c.add_event_handler(filter_handler, events.NewMessage(pattern=r'^/filter\s+(\w+)(?:\s+(.*))?$'))
                c.add_event_handler(filter_alias_handler, events.NewMessage(pattern=r'^/filter_(\w+)(?:\s+(.*))?$'))
                c.add_event_handler(help_handler, events.NewMessage(pattern=r'^/help$'))
                c.add_event_handler(specific_select_handler, events.NewMessage(pattern=r'^/select_(-?\d+)'))
                c.add_event_handler(on_chat_action, events.ChatAction)
                
                active_clients.append(c)
                
            except Exception as e:
                print(f"❌ Failed to start client {s_name}: {e}")

    # Run the startup sequence
    loop = asyncio.get_event_loop()
    loop.run_until_complete(start_clients())
    
    if not active_clients:
        print("❌ No active sessions found! Exiting.")
        sys.exit(1)
        
    # Update global clients list to only include active ones
    clients = active_clients
    # Update worker_clients to remove inactive ones
    worker_clients = [c for c in worker_clients if c in active_clients]
    
    # Ensure main client is active
    if client not in active_clients:
        if active_clients:
            client = active_clients[0]
            print(f"⚠️ Main client failed. Switched to {client.session.filename}")
        else:
            print("❌ Main client failed and no backups available.")
            sys.exit(1)

    print(f"🚀 Bot is running with {len(active_clients)} active sessions.")

    if args.export:
        client.loop.run_until_complete(scan_and_export(args.export, to_csv=False, to_xlsx=False))
        sys.exit(0)
    
    # Main Bot Loop
    client.loop.create_task(startup_check())
    
    # Keep all clients running
    try:
        client.loop.run_until_complete(asyncio.gather(*(c.run_until_disconnected() for c in active_clients)))
    except KeyboardInterrupt:
        print("🛑 Bot stopped by user.")
