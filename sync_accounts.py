#!/usr/bin/env python3
import os
import sys
import json
import logging
import asyncio
from aiohttp import ClientSession, ClientTimeout
from dotenv import load_dotenv
from tqdm.asyncio import tqdm
from urllib.parse import quote
import argparse

# ==========================
# INITIALIZATION & LOGGING
# ==========================
load_dotenv()

logging.basicConfig(
    filename='account_sync.log',
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(message)s'))
logging.getLogger().addHandler(console_handler)

# ==========================
# ENVIRONMENT & SETUP
# ==========================

def prompt_env_variable(var_name, prompt_text):
    """Prompt user for a variable and return it."""
    value = input(f"{prompt_text}: ").strip()
    if not value:
        print(f"❌ {var_name} cannot be empty.")
        sys.exit(1)
    return value

def ensure_env_file():
    """Create .env if missing and prompt user for connection details."""
    env_path = ".env"
    if not os.path.exists(env_path):
        print("🔧 No .env file found. Let's create one.\nPlease enter your ERPNext connection details:")

        source_url = prompt_env_variable("SOURCE_URL", "Enter Source ERPNext URL (e.g. https://source-erp.com)")
        source_key = prompt_env_variable("SOURCE_KEY", "Enter Source API Key")
        source_secret = prompt_env_variable("SOURCE_SECRET", "Enter Source API Secret")
        target_url = prompt_env_variable("TARGET_URL", "Enter Target ERPNext URL (e.g. https://target-erp.com)")
        target_key = prompt_env_variable("TARGET_KEY", "Enter Target API Key")
        target_secret = prompt_env_variable("TARGET_SECRET", "Enter Target API Secret")

        with open(env_path, "w") as f:
            f.write(f"SOURCE_URL={source_url}\n")
            f.write(f"SOURCE_KEY={source_key}\n")
            f.write(f"SOURCE_SECRET={source_secret}\n")
            f.write(f"TARGET_URL={target_url}\n")
            f.write(f"TARGET_KEY={target_key}\n")
            f.write(f"TARGET_SECRET={target_secret}\n")

        print("✅ .env file created successfully.")
        # ensure .gitignore entry
        if not os.path.exists(".gitignore") or ".env" not in open(".gitignore").read():
            with open(".gitignore", "a") as gitignore:
                gitignore.write("\n.env\n")
            print("✅ Added .env to .gitignore.")
    else:
        print("✅ Using existing .env file.")

    load_dotenv()


def get_env(var_name, prompt_if_missing=False):
    val = os.getenv(var_name)
    if not val and prompt_if_missing:
        val = prompt_env_variable(var_name, f"Enter value for {var_name}")
        with open(".env", "a") as f:
            f.write(f"{var_name}={val}\n")
    return val


# ==========================
# HELPERS
# ==========================
def normalize_name(name):
    """Strip leading numbers/dashes for comparison"""
    if not name:
        return ""
    parts = name.split("-", 1)
    if len(parts) == 2 and parts[0].strip().isdigit():
        return parts[1].strip()
    return name.strip()

def compare_accounts(src, tgt):
    fields = ["account_name", "parent_account", "account_type", "root_type", "report_type", "is_group", "company"]
    diffs = {}
    for f in fields:
        src_val = src.get(f)
        tgt_val = tgt.get(f)
        if f == "parent_account":
            src_val = normalize_name(src_val) if src_val else None
            tgt_val = normalize_name(tgt_val) if tgt_val else None
        if src_val != tgt_val:
            diffs[f] = (src.get(f), tgt.get(f))
    return diffs


# ==========================
# ASYNC ERPNext REQUESTS
# ==========================
async def frappe_request(session, method, url, api_key, api_secret, endpoint, data=None):
    headers = {"Authorization": f"token {api_key}:{api_secret}", "Content-Type": "application/json"}
    full_url = f"{url}/api/resource/{endpoint}"
    try:
        async with session.request(method, full_url, headers=headers, json=data) as resp:
            if resp.status not in (200, 201):
                text = await resp.text()
                logging.error(f"{method} {endpoint} failed: {resp.status} - {text}")
                return None
            if resp.content_type == "application/json":
                j = await resp.json()
                return j.get("data")
            return None
    except Exception as e:
        logging.error(f"{method} {endpoint} failed: {e}")
        return None


async def frappe_get_doc(session, url, api_key, api_secret, doctype, name):
    return await frappe_request(session, "GET", url, api_key, api_secret, f"{doctype}/{quote(name)}")


async def frappe_post_doc(session, url, api_key, api_secret, doctype, data):
    return await frappe_request(session, "POST", url, api_key, api_secret, doctype, data)


async def frappe_put_doc(session, url, api_key, api_secret, doctype, name, data):
    return await frappe_request(session, "PUT", url, api_key, api_secret, f"{doctype}/{quote(name)}", data)


async def frappe_get_all(session, url, api_key, api_secret, doctype, company=None, limit=10000):
    headers = {"Authorization": f"token {api_key}:{api_secret}"}
    params = {"limit_page_length": limit, "fields": '["name","company"]'}
    async with session.get(f"{url}/api/resource/{doctype}", headers=headers, params=params) as resp:
        resp.raise_for_status()
        j = await resp.json()
        if company:
            return [d["name"] for d in j.get("data", []) if d.get("company") == company]
        return [d["name"] for d in j.get("data", [])]


# ==========================
# PARENT HANDLING
# ==========================
async def ensure_parent(session, target_url, target_key, target_secret, parent_name, target_lookup, dry_run):
    if not parent_name:
        return None
    norm_parent = normalize_name(parent_name)
    if norm_parent in target_lookup:
        doc = target_lookup[norm_parent]
        return doc["name"] if isinstance(doc, dict) and "name" in doc else norm_parent

    logging.info(f"Parent account '{parent_name}' missing, creating...")
    parent_data = {
        "account_name": norm_parent,
        "is_group": 1,
        "root_type": "Asset",
        "account_type": "",
        "parent_account": None,
        "company": ""
    }

    if dry_run:
        logging.info(f"[Dry-run] Would create parent: {parent_name}")
        target_lookup[norm_parent] = norm_parent
        return norm_parent

    created = await frappe_post_doc(session, target_url, target_key, target_secret, "Account", parent_data)
    if created:
        target_lookup[norm_parent] = created
        return created["name"]
    logging.error(f"Failed to create parent account: {parent_name}")
    return None


# ==========================
# ACCOUNT SYNC
# ==========================
async def sync_account(session, src_doc, target_url, target_key, target_secret, target_lookup, dry_run):
    parent_name = src_doc.get("parent_account")
    if parent_name:
        ensured_parent = await ensure_parent(session, target_url, target_key, target_secret, parent_name, target_lookup, dry_run)
        if not ensured_parent:
            logging.error(f"Skipping {src_doc.get('name')} due to missing parent")
            return
        src_doc["parent_account"] = ensured_parent

    tgt_doc = await frappe_get_doc(session, target_url, target_key, target_secret, "Account", src_doc["name"])
    if tgt_doc:
        diffs = compare_accounts(src_doc, tgt_doc)
        if diffs:
            logging.info(f"Update: {src_doc['name']} diffs={diffs}")
            print(f"Update: {src_doc['name']} diffs={diffs}")
            if not dry_run:
                update_data = {f: src_doc[f] for f in diffs.keys()}
                await frappe_put_doc(session, target_url, target_key, target_secret, "Account", src_doc["name"], update_data)
        else:
            logging.info(f"Skip: {src_doc['name']} (no changes)")
            print(f"Skip: {src_doc['name']} (no changes)")
    else:
        logging.info(f"Create: {src_doc['name']}")
        print(f"Create: {src_doc['name']}")
        if not dry_run:
            await frappe_post_doc(session, target_url, target_key, target_secret, "Account", src_doc)


# ==========================
# MAIN
# ==========================
async def main():
    parser = argparse.ArgumentParser(description="Sync ERPNext Accounts between instances")
    parser.add_argument('--dry-run', action='store_true', help='Simulate without modifying target')
    parser.add_argument('--company', help='Filter by company name')
    args = parser.parse_args()

    ensure_env_file()
    SOURCE_URL = get_env("SOURCE_URL", True)
    SOURCE_KEY = get_env("SOURCE_KEY", True)
    SOURCE_SECRET = get_env("SOURCE_SECRET", True)
    TARGET_URL = get_env("TARGET_URL", True)
    TARGET_KEY = get_env("TARGET_KEY", True)
    TARGET_SECRET = get_env("TARGET_SECRET", True)

    logging.info("Starting account sync...")
    print("Starting account sync...")

    timeout = ClientTimeout(total=30)
    async with ClientSession(timeout=timeout) as session:
        source_accounts = await frappe_get_all(session, SOURCE_URL, SOURCE_KEY, SOURCE_SECRET, "Account", args.company)
        logging.info(f"Total source accounts fetched: {len(source_accounts)}")
        print(f"Total accounts to process: {len(source_accounts)}")

        target_lookup = {}
        target_names = await frappe_get_all(session, TARGET_URL, TARGET_KEY, TARGET_SECRET, "Account", args.company)
        for n in target_names:
            target_lookup[normalize_name(n)] = n

        async def fetch_and_sync(name):
            src_doc = await frappe_get_doc(session, SOURCE_URL, SOURCE_KEY, SOURCE_SECRET, "Account", name)
            if not src_doc:
                logging.warning(f"Could not fetch {name} from source")
                return
            for f in ["account_currency", "balance", "total_debit", "total_credit", "creation", "modified",
                      "modified_by", "owner", "idx", "docstatus"]:
                src_doc.pop(f, None)
            await sync_account(session, src_doc, TARGET_URL, TARGET_KEY, TARGET_SECRET, target_lookup, args.dry_run)

        tasks = [fetch_and_sync(name) for name in source_accounts]
        for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Processing accounts", unit="acct"):
            await f

    logging.info("Sync complete.")
    print("🎉 Sync complete.")


if __name__ == "__main__":
    asyncio.run(main())
