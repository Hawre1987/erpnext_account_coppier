#!/usr/bin/env python3
"""
sync_accounts_v2.py

Hierarchy-aware, cautious ERPNext Account synchronizer (source -> target).

Behaviors:
 - If .env missing => interactive step-by-step prompts to create it (SOURCE/TARGET urls + keys)
 - .env will be added to .gitignore automatically
 - Ignores leading account numbers when matching accounts between instances
 - Processes accounts in depth order (parents before children) sequentially (safe)
 - --dry-run will only show actions (no POST/PUT)
 - --company filter available
 - Leaves account_currency and balances untouched (never copied)
 - Logs actions to account_sync_v2.log
"""
import os
import sys
import re
import time
import json
import logging
import argparse
from urllib.parse import quote
from collections import defaultdict, deque

import requests
from dotenv import load_dotenv
from tqdm import tqdm

# -------------------------
# Logging setup
# -------------------------
LOGFILE = "account_sync_v2.log"
logging.basicConfig(
    filename=LOGFILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(message)s"))
logging.getLogger().addHandler(console)

# -------------------------
# Utilities: env & prompting
# -------------------------
ENV_PATH = ".env"
GITIGNORE_PATH = ".gitignore"

def add_env_to_gitignore():
    try:
        content = ""
        if os.path.exists(GITIGNORE_PATH):
            with open(GITIGNORE_PATH, "r", encoding="utf-8") as f:
                content = f.read()
        if ".env" not in content.splitlines():
            with open(GITIGNORE_PATH, "a", encoding="utf-8") as f:
                if not content.endswith("\n") and content != "":
                    f.write("\n")
                f.write(".env\n")
            logging.info("Added .env to .gitignore")
    except Exception as e:
        logging.warning(f"Could not update .gitignore: {e}")

def prompt_env_value(varname, prompt_text):
    v = None
    try:
        v = input(f"{prompt_text}: ").strip()
    except KeyboardInterrupt:
        print()
        logging.error("Interrupted by user")
        sys.exit(1)
    if not v:
        logging.error(f"{varname} cannot be empty.")
        sys.exit(1)
    return v

def ensure_env_file():
    """
    If .env doesn't exist, interactively prompt user for SOURCE/TARGET credentials and write .env.
    If exists, just load it.
    """
    if os.path.exists(ENV_PATH):
        load_dotenv(ENV_PATH)
        logging.log("------------------------------------------------------------------")
        logging.log("******************************************************************")
        logging.log("------------------------------------------------------------------")
        logging.info("Using existing .env file.")
        return

    print("No .env found — let's create one. (Your API keys/secrets will be stored in .env and .env will be excluded from git.)")
    source_url = prompt_env_value("SOURCE_URL", "Source ERPNext URL (e.g. https://source.example.com)")
    source_key = prompt_env_value("SOURCE_KEY", "Source API Key")
    source_secret = prompt_env_value("SOURCE_SECRET", "Source API Secret")
    target_url = prompt_env_value("TARGET_URL", "Target ERPNext URL (e.g. https://target.example.com)")
    target_key = prompt_env_value("TARGET_KEY", "Target API Key")
    target_secret = prompt_env_value("TARGET_SECRET", "Target API Secret")

    with open(ENV_PATH, "w", encoding="utf-8") as f:
        f.write(f"SOURCE_URL={source_url}\n")
        f.write(f"SOURCE_KEY={source_key}\n")
        f.write(f"SOURCE_SECRET={source_secret}\n")
        f.write(f"TARGET_URL={target_url}\n")
        f.write(f"TARGET_KEY={target_key}\n")
        f.write(f"TARGET_SECRET={target_secret}\n")

    add_env_to_gitignore()
    load_dotenv(ENV_PATH)
    logging.info(".env created and loaded.")


def get_env(varname, prompt_if_missing=False):
    val = os.getenv(varname)
    if not val and prompt_if_missing:
        val = prompt_env_value(varname, f"Enter {varname}")
        # append to .env
        with open(ENV_PATH, "a", encoding="utf-8") as f:
            f.write(f"{varname}={val}\n")
    return val

# -------------------------
# Name normalization (ignore leading numbers)
# -------------------------
_leading_number_re = re.compile(r"^\s*\d+[\s\-\._:]+(.*)$")

def normalize_name(name):
    if not name:
        return ""
    # strip whitespace
    n = name.strip()
    # remove leading numeric prefix like "1000 - Account Name"
    m = _leading_number_re.match(n)
    if m:
        n = m.group(1).strip()
    # collapse multiple spaces, lower-case for matching
    n = re.sub(r"\s+", " ", n)
    return n.lower()

# -------------------------
# REST helpers
# -------------------------
def api_headers(key, secret):
    return {"Authorization": f"token {key}:{secret}", "Content-Type": "application/json"}

def get_all_accounts(base_url, api_key, api_secret, company=None, page_limit=10000):
    """
    Returns list of account dicts (with at least name and parent_account and company)
    """
    url = base_url.rstrip("/") + "/api/resource/Account"
    params = {"limit_page_length": page_limit, "fields": '["name","parent_account","account_name","is_group","company","account_type","root_type","report_type"]'}
    try:
        r = requests.get(url, headers=api_headers(api_key, api_secret), params=params, timeout=60)
        r.raise_for_status()
        j = r.json()
        data = j.get("data") or []
        if company:
            data = [d for d in data if d.get("company") == company]
        return data
    except requests.RequestException as e:
        logging.error(f"Failed fetching accounts from {base_url}: {e} - {getattr(e, 'response', '')}")
        raise

def get_account(base_url, api_key, api_secret, name):
    url = base_url.rstrip("/") + f"/api/resource/Account/{quote(name, safe='')}"
    try:
        r = requests.get(url, headers=api_headers(api_key, api_secret), timeout=30)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json().get("data")
    except requests.RequestException as e:
        logging.error(f"GET Account/{name} failed: {e}")
        return None

def create_account(base_url, api_key, api_secret, payload):
    url = base_url.rstrip("/") + "/api/resource/Account"
    try:
        r = requests.post(url, headers=api_headers(api_key, api_secret), json=payload, timeout=30)
        if r.status_code in (200, 201):
            return r.json().get("data")
        logging.error(f"Create failed ({r.status_code}): {r.text}")
        return None
    except requests.RequestException as e:
        logging.error(f"Create account failed: {e}")
        return None

def update_account(base_url, api_key, api_secret, name, payload):
    url = base_url.rstrip("/") + f"/api/resource/Account/{quote(name, safe='')}"
    try:
        r = requests.put(url, headers=api_headers(api_key, api_secret), json=payload, timeout=30)
        if r.status_code in (200, 201):
            return r.json().get("data")
        logging.error(f"Update failed ({r.status_code}): {r.text}")
        return None
    except requests.RequestException as e:
        logging.error(f"Update account failed: {e}")
        return None

# -------------------------
# Compare/prepare functions
# -------------------------
COMPARE_FIELDS = ["account_name", "parent_account", "account_type", "root_type", "report_type", "is_group", "company"]

def prepare_source_doc_for_transfer(src):
    # Copy allowed fields, but remove balance/currency fields
    doc = {}
    # We keep 'name' as the unique identifier to attempt creation/update (ERPNext commonly uses name=account_name unless with numbering)
    for k in ("name", "account_name", "parent_account", "is_group", "account_type", "root_type", "report_type", "company"):
        if k in src:
            doc[k] = src[k]
    # Ensure we never send balances or account_currency
    # Explicitly: do NOT include account_currency, balance, total_debit, total_credit
    return doc

def account_differences(src_doc, tgt_doc):
    diffs = {}
    for f in COMPARE_FIELDS:
        s = src_doc.get(f)
        t = tgt_doc.get(f)
        # compare normalized parent_account specially
        if f == "parent_account":
            s_norm = normalize_name(s) if s else None
            t_norm = normalize_name(t) if t else None
            if s_norm != t_norm:
                diffs[f] = (s, t)
        else:
            if (s or "") != (t or ""):
                diffs[f] = (s, t)
    return diffs

# -------------------------
# Hierarchy helpers
# -------------------------
def build_source_graph(src_accounts):
    """
    Build maps:
     - name_to_doc (original names)
     - normalized_to_names -> list of original names that normalize to same key
     - children mapping
    """
    name_to_doc = {a["name"]: a for a in src_accounts}
    norm_to_name = defaultdict(list)
    for n, d in name_to_doc.items():
        norm = normalize_name(d.get("name") or d.get("account_name") or n)
        norm_to_name[norm].append(n)

    children = defaultdict(list)
    roots = []
    for n, d in name_to_doc.items():
        parent = d.get("parent_account")
        if parent:
            # parent may have numbers; we store edge by original names (if available)
            children[parent].append(n)
        else:
            roots.append(n)
    return name_to_doc, norm_to_name, children

def compute_depths(name_to_doc):
    """
    Compute depth (distance from root) using parent_account links in source
    Return dict name->depth. Unresolvable parents produce deeper depth.
    """
    depths = {}
    def depth(name, stack=None):
        if name in depths:
            return depths[name]
        if stack is None:
            stack = set()
        if name in stack:
            # cycle
            logging.warning(f"Cycle detected in parent chain for {name}; breaking")
            depths[name] = 0
            return 0
        stack.add(name)
        doc = name_to_doc.get(name)
        parent = doc.get("parent_account") if doc else None
        # if parent is None or empty => depth 0
        if not parent:
            depths[name] = 0
            stack.remove(name)
            return 0
        # parent might be present by exact name or only normalized in source; try to find parent's exact name from name_to_doc
        parent_exact = parent if parent in name_to_doc else None
        # fallback: try normalized match
        if not parent_exact:
            pnorm = normalize_name(parent)
            # find candidate
            parent_exact = None
            for cand, cand_doc in name_to_doc.items():
                if normalize_name(cand_doc.get("name") or cand_doc.get("account_name") or cand) == pnorm:
                    parent_exact = cand
                    break
        if not parent_exact:
            # parent not found in source; treat as root-like (depth 0) but mark by using unique depth 1
            depths[name] = 1
            stack.remove(name)
            return 1
        d = 1 + depth(parent_exact, stack)
        depths[name] = d
        stack.remove(name)
        return d

    # build local name_to_doc closure
    for n in list(name_to_doc.keys()):
        if n not in depths:
            depth(n)
    return depths

# -------------------------
# Sync algorithm (sequential by depth)
# -------------------------
def sync_all(source_url, source_key, source_secret, target_url, target_key, target_secret, dry_run=False, company=None, max_parent_retries=5, retry_delay=1.0):
    logging.info("Fetching source accounts...")
    source_accounts = get_all_accounts(source_url, source_key, source_secret, company=company)
    logging.info(f"Fetched {len(source_accounts)} accounts from source")

    logging.info("Fetching target accounts (names)...")
    target_accounts = get_all_accounts(target_url, target_key, target_secret, company=company)
    logging.info(f"Fetched {len(target_accounts)} accounts from target")

    # Build quick lookup of target normalized names -> account full doc
    target_norm_lookup = {}
    for a in target_accounts:
        n = a.get("name") or a.get("account_name")
        if not n:
            continue
        target_norm_lookup[normalize_name(n)] = a

    # Build source maps
    name_to_doc = {a["name"]: a for a in source_accounts}
    # compute depths
    # we rely on parent_account values from source; normalize matching will be used against target
    def get_parent_name_in_source(name):
        doc = name_to_doc.get(name)
        return doc.get("parent_account") if doc else None

    depths = {}
    # compute depths iteratively (safe)
    def compute_depth_for(name, visited=None):
        if name in depths:
            return depths[name]
        if visited is None:
            visited = set()
        if name in visited:
            depths[name] = 0
            return 0
        visited.add(name)
        doc = name_to_doc.get(name)
        if not doc:
            depths[name] = 0
            return 0
        parent = doc.get("parent_account")
        if not parent:
            depths[name] = 0
            return 0
        # try to find parent's actual key in source (exact name or normalized)
        parent_key = None
        if parent in name_to_doc:
            parent_key = parent
        else:
            pnorm = normalize_name(parent)
            for cand, cd in name_to_doc.items():
                if normalize_name(cd.get("name") or cd.get("account_name") or cand) == pnorm:
                    parent_key = cand
                    break
        if not parent_key:
            # parent not present in source => treat as shallow (depth 1)
            depths[name] = 1
            return 1
        d = 1 + compute_depth_for(parent_key, visited)
        depths[name] = d
        return d

    for nm in list(name_to_doc.keys()):
        compute_depth_for(nm)

    # Group accounts by depth sorted ascending
    depth_buckets = defaultdict(list)
    for n, d in depths.items():
        depth_buckets[d].append(n)
    sorted_depths = sorted(depth_buckets.keys())

    total_accounts = len(name_to_doc)
    processed = 0
    pbar = tqdm(total=total_accounts, desc="Processing accounts", unit="acct")

    # When creating parents in target, we will add them to target_norm_lookup so children find them
    for depth in sorted_depths:
        logging.info(f"Processing depth {depth} ({len(depth_buckets[depth])} accounts)")
        for src_name in depth_buckets[depth]:
            processed += 1
            try:
                src_doc_raw = name_to_doc[src_name]
                src_doc = prepare_source_doc_for_transfer(src_doc_raw)
                # remove currency/balance if present (defensive)
                for f in ("account_currency", "balance", "total_debit", "total_credit"):
                    src_doc.pop(f, None)

                src_norm = normalize_name(src_doc.get("name") or src_doc.get("account_name") or src_name)

                # Check if account exists in target (by normalized name)
                tgt = target_norm_lookup.get(src_norm)
                if tgt:
                    # compare
                    tgt_full = get_account(target_url, target_key, target_secret, tgt["name"])
                    if not tgt_full:
                        logging.warning(f"Target returned no full doc for {tgt['name']}, treating as missing")
                        tgt_full = None
                else:
                    tgt_full = None

                # ensure parent exists in target first (if any)
                parent = src_doc.get("parent_account")
                if parent:
                    parent_norm = normalize_name(parent)
                    parent_in_target = target_norm_lookup.get(parent_norm)
                    if not parent_in_target:
                        # parent missing in target -> attempt to create parent chain from source if available
                        # find parent doc in source (exact or normalized)
                        parent_src_key = parent if parent in name_to_doc else None
                        if not parent_src_key:
                            # try normalized match
                            for cand, cd in name_to_doc.items():
                                if normalize_name(cd.get("name") or cd.get("account_name") or cand) == parent_norm:
                                    parent_src_key = cand
                                    break
                        parent_created = False
                        attempts = 0
                        while not parent_created and attempts < max_parent_retries:
                            attempts += 1
                            # Build parent payload either from source (best) or minimal group fallback
                            if parent_src_key:
                                parent_src_doc = prepare_source_doc_for_transfer(name_to_doc[parent_src_key])
                                parent_payload = parent_src_doc
                                # ensure it is a group to allow children
                                parent_payload["is_group"] = 1 if parent_payload.get("is_group") else 1
                                parent_payload.setdefault("company", "")
                                parent_payload.pop("account_currency", None)
                            else:
                                # fallback
                                logging.info(f"Parent '{parent}' not found in source; creating minimal group entry in target")
                                parent_payload = {
                                    "account_name": parent.strip(),
                                    "is_group": 1,
                                    "parent_account": None,
                                    "company": ""
                                }
                            if dry_run:
                                logging.info(f"[Dry-run] Would create parent: {parent_payload.get('account_name')}")
                                # pretend it created and add to lookup
                                target_norm_lookup[parent_norm] = {"name": parent_payload.get("account_name")}
                                parent_created = True
                                break
                            else:
                                created = create_account(target_url, target_key, target_secret, parent_payload)
                                if created:
                                    logging.info(f"Created parent '{created.get('name')}' (from {parent})")
                                    # insert into lookup
                                    target_norm_lookup[parent_norm] = created
                                    parent_created = True
                                    break
                                else:
                                    logging.warning(f"Attempt {attempts} to create parent '{parent}' failed; retrying in {retry_delay}s")
                                    time.sleep(retry_delay)
                        if not parent_created:
                            logging.error(f"Failed to ensure parent '{parent}' for account '{src_name}' after {max_parent_retries} attempts. Skipping account.")
                            pbar.update(1)
                            continue  # skip this account
                    else:
                        # parent present - nothing to do
                        pass

                # Now either update or create the account in target
                if tgt_full:
                    # compare differences (excluding currency/balance)
                    diffs = account_differences(src_doc, tgt_full)
                    if diffs:
                        logging.info(f"Update required for '{src_doc.get('name')}' diffs={diffs}")
                        print(f"Update: {src_doc.get('name')} diffs={diffs}")
                        if dry_run:
                            logging.info(f"[Dry-run] Would update '{tgt_full.get('name')}' with {list(diffs.keys())}")
                        else:
                            update_payload = {}
                            for k in diffs.keys():
                                # for parent_account, use parent's actual name in target (if present)
                                if k == "parent_account":
                                    desired_parent = src_doc.get("parent_account")
                                    if desired_parent:
                                        desired_parent_norm = normalize_name(desired_parent)
                                        parent_in_target = target_norm_lookup.get(desired_parent_norm)
                                        if isinstance(parent_in_target, dict) and parent_in_target.get("name"):
                                            update_payload["parent_account"] = parent_in_target["name"]
                                        else:
                                            # send raw parent string as fallback
                                            update_payload["parent_account"] = desired_parent
                                else:
                                    update_payload[k] = src_doc.get(k)
                            updated = update_account(target_url, target_key, target_secret, tgt_full.get("name"), update_payload)
                            if updated:
                                logging.info(f"Updated '{tgt_full.get('name')}' successfully.")
                            else:
                                logging.error(f"Failed to update '{tgt_full.get('name')}'.")
                    else:
                        logging.info(f"Skip: {src_doc.get('name')} (no changes)")
                else:
                    # create
                    logging.info(f"Create: {src_doc.get('name')}")
                    print(f"Create: {src_doc.get('name')}")
                    # for create, ensure parent value uses actual target parent name (if exists)
                    if src_doc.get("parent_account"):
                        desired_parent_norm = normalize_name(src_doc.get("parent_account"))
                        parent_in_target = target_norm_lookup.get(desired_parent_norm)
                        if parent_in_target and isinstance(parent_in_target, dict) and parent_in_target.get("name"):
                            src_doc["parent_account"] = parent_in_target["name"]
                        # else keep original string (ERPNext will raise if parent missing)
                    if dry_run:
                        logging.info(f"[Dry-run] Would create account: {src_doc.get('name')}")
                        # pretend created
                        target_norm_lookup[src_norm] = {"name": src_doc.get("name")}
                    else:
                        created = create_account(target_url, target_key, target_secret, src_doc)
                        if created:
                            logging.info(f"Created '{created.get('name')}'")
                            target_norm_lookup[src_norm] = created
                        else:
                            logging.error(f"Failed to create '{src_doc.get('name')}'")
            except Exception as exc:
                logging.exception(f"Unhandled exception syncing {src_name}: {exc}")
            finally:
                pbar.update(1)

    pbar.close()
    logging.info("Sync run complete.")


# -------------------------
# CLI entrypoint
# -------------------------
def main():
    parser = argparse.ArgumentParser(description="ERPNext Accounts sync (hierarchy-aware, safer sequential)")
    parser.add_argument("--dry-run", action="store_true", help="Simulate actions without applying to the target")
    parser.add_argument("--company", help="Filter accounts by company name")
    parser.add_argument("--max-parent-retries", type=int, default=5, help="Max attempts to create missing parent accounts")
    parser.add_argument("--retry-delay", type=float, default=1.0, help="Delay between parent create retries (seconds)")
    args = parser.parse_args()

    ensure_env_file()
    # required envs
    SOURCE_URL = get_env("SOURCE_URL", True)
    SOURCE_KEY = get_env("SOURCE_KEY", True)
    SOURCE_SECRET = get_env("SOURCE_SECRET", True)
    TARGET_URL = get_env("TARGET_URL", True)
    TARGET_KEY = get_env("TARGET_KEY", True)
    TARGET_SECRET = get_env("TARGET_SECRET", True)

    logging.info("Starting sync_accounts_v2.py")
    start = time.time()
    sync_all(
        SOURCE_URL, SOURCE_KEY, SOURCE_SECRET,
        TARGET_URL, TARGET_KEY, TARGET_SECRET,
        dry_run=args.dry_run,
        company=args.company,
        max_parent_retries=args.max_parent_retries,
        retry_delay=args.retry_delay,
    )
    logging.info(f"Finished in {time.time() - start:.2f}s")

if __name__ == "__main__":
    main()
