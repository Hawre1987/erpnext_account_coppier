#!/usr/bin/env python3
import os
import sys
import json
import logging
import requests
from dotenv import load_dotenv


# ==========================
# INITIALIZATION & LOGGING
# ==========================
load_dotenv()

logging.basicConfig(
    filename='account_sync.log',
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)


# ==========================
# FUNCTIONS
# ==========================
def prompt_env():
    """Prompt user for ERPNext environment variables and save to .env"""
    print("\n🔐 Environment setup:")
    source_url = input("Enter SOURCE ERPNext URL: ").strip()
    source_key = input("Enter SOURCE API KEY: ").strip()
    source_secret = input("Enter SOURCE API SECRET: ").strip()
    target_url = input("Enter TARGET ERPNext URL: ").strip()
    target_key = input("Enter TARGET API KEY: ").strip()
    target_secret = input("Enter TARGET API SECRET: ").strip()

    with open('.env', 'w') as f:
        f.write(f"SOURCE_URL={source_url}\n")
        f.write(f"SOURCE_KEY={source_key}\n")
        f.write(f"SOURCE_SECRET={source_secret}\n")
        f.write(f"TARGET_URL={target_url}\n")
        f.write(f"TARGET_KEY={target_key}\n")
        f.write(f"TARGET_SECRET={target_secret}\n")

    print("✅ Environment variables saved to .env (excluded in .gitignore).")
    sys.exit(0)


def get_env(var_name, prompt_if_missing=False):
    """Get environment variable or prompt if missing"""
    val = os.getenv(var_name)
    if not val and prompt_if_missing:
        print(f"Missing environment variable: {var_name}")
        prompt_env()
    return val


def frappe_request(method, url, api_key, api_secret, endpoint, data=None):
    """Generic Frappe API request handler"""
    headers = {
        "Authorization": f"token {api_key}:{api_secret}",
        "Content-Type": "application/json",
    }
    full_url = f"{url}/api/resource/{endpoint}"

    resp = requests.request(method, full_url, headers=headers, json=data)

    if resp.status_code not in (200, 201):
        if resp.status_code == 404:
            return None
        logging.error(f"{method} {endpoint} failed: {resp.status_code} - {resp.text}")
        return None

    return resp.json().get("data") if resp.text else None


def frappe_get_doc(url, api_key, api_secret, doctype, name):
    return frappe_request("GET", url, api_key, api_secret, f"{doctype}/{name}")


def frappe_post_doc(url, api_key, api_secret, doctype, data):
    return frappe_request("POST", url, api_key, api_secret, doctype, data)


def frappe_put_doc(url, api_key, api_secret, doctype, name, data):
    return frappe_request("PUT", url, api_key, api_secret, f"{doctype}/{name}", data)


def frappe_get_all(url, api_key, api_secret, doctype, company_filter=None, limit=10000):
    """Retrieve all documents of a given doctype"""
    headers = {"Authorization": f"token {api_key}:{api_secret}"}
    params = {"limit_page_length": limit, "fields": '["name"]'}

    if company_filter:
        params["filters"] = json.dumps([["company", "=", company_filter]])

    resp = requests.get(f"{url}/api/resource/{doctype}", headers=headers, params=params)
    resp.raise_for_status()
    return [d["name"] for d in resp.json().get("data", [])]


def compare_accounts(src, tgt):
    """Compare account fields between source and target"""
    fields = [
        "account_name",
        "parent_account",
        "account_type",
        "root_type",
        "report_type",
        "is_group",
        "company",
    ]
    differences = {}
    for f in fields:
        if src.get(f) != tgt.get(f):
            differences[f] = (src.get(f), tgt.get(f))
    return differences


# ==========================
# MAIN SCRIPT
# ==========================
def main():
    import argparse

    parser = argparse.ArgumentParser(description="Sync ERPNext Accounts between instances.")
    parser.add_argument('--dry-run', action='store_true', help='Simulate actions without writing changes')
    parser.add_argument('--company', type=str, default=None, help='Filter by company name')
    args = parser.parse_args()

    SOURCE_URL = get_env("SOURCE_URL", True)
    SOURCE_KEY = get_env("SOURCE_KEY", True)
    SOURCE_SECRET = get_env("SOURCE_SECRET", True)
    TARGET_URL = get_env("TARGET_URL", True)
    TARGET_KEY = get_env("TARGET_KEY", True)
    TARGET_SECRET = get_env("TARGET_SECRET", True)

    logging.info("Starting account sync...")

    source_accounts = frappe_get_all(SOURCE_URL, SOURCE_KEY, SOURCE_SECRET, "Account", args.company)

    for name in source_accounts:
        src_doc = frappe_get_doc(SOURCE_URL, SOURCE_KEY, SOURCE_SECRET, "Account", name)
        if not src_doc:
            logging.warning(f"Could not fetch {name} from source.")
            continue

        # Remove irrelevant fields
        for f in ["account_currency", "balance", "total_debit", "total_credit"]:
            src_doc.pop(f, None)

        for k in list(src_doc.keys()):
            if k.startswith("_") or k in ["creation", "modified", "modified_by", "owner", "idx", "docstatus"]:
                src_doc.pop(k, None)

        tgt_doc = frappe_get_doc(TARGET_URL, TARGET_KEY, TARGET_SECRET, "Account", name)

        if not tgt_doc:
            logging.info(f"Create: {name}")
            if not args.dry_run:
                frappe_post_doc(TARGET_URL, TARGET_KEY, TARGET_SECRET, "Account", src_doc)
        else:
            diffs = compare_accounts(src_doc, tgt_doc)
            if diffs:
                logging.info(f"Update: {name} diffs={diffs}")
                if not args.dry_run:
                    update_data = {f: src_doc[f] for f in diffs.keys()}
                    frappe_put_doc(TARGET_URL, TARGET_KEY, TARGET_SECRET, "Account", name, update_data)
            else:
                logging.info(f"Skip: {name} (no changes)")

    logging.info("Sync complete.")


if __name__ == '__main__':
    main()
