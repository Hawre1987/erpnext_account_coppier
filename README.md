# ERPNext Account Sync Tool

This script synchronizes Chart of Accounts between two ERPNext instances.

---

## ⚙️ Features

* Copy accounts that don't exist in the target.
* Update existing ones if fields differ (ignores balance and currency).
* Option to run safely with `--dry-run`.
* Logging to `account_sync.log`.
* Optional filter by company.
* Secrets stored securely in `.env`.

---

## 🧠 Preparation

Before using this script, ensure the following:

1. You have API access enabled in both ERPNext instances (Source & Target).
2. You know your API Key and Secret for both systems.
3. You have Python 3.8+ installed.

### Install dependencies

```bash
pip install -r requirements.txt
```

### Create `.env` file

The script will prompt you automatically to enter credentials on the first run:

```bash
python3 sync_accounts.py
```

This will save your credentials securely in `.env`, which is excluded from version control.

Example `.env` content:

```env
SOURCE_URL=https://source-erp.example.com
SOURCE_KEY=xxxx
SOURCE_SECRET=xxxx
TARGET_URL=https://target-erp.example.com
TARGET_KEY=xxxx
TARGET_SECRET=xxxx
```

---

## 🚀 Usage

### Run normally

```bash
python3 sync_accounts.py
```

### Dry-run (no changes applied)

```bash
python3 sync_accounts.py --dry-run
```

### Filter by company

```bash
python3 sync_accounts.py --company "My Company Name"
```

### Logs

All actions are logged in `account_sync.log`.

---

## 📜 Disclaimer

> ⚠️ **Use at your own risk.** This tool directly interacts with ERPNext instances and can modify financial account data. Always create a **full database backup** before running the synchronization.
>
> The author assumes **no responsibility or liability** for any data loss, corruption, or system issues resulting from the use of this tool. By using this script, you acknowledge that you do so entirely **at your own risk**.

---

## 📂 Project Files

* `sync_accounts.py` — main Python script.
* `.env` — environment variables (auto-created).
* `.gitignore` — excludes sensitive files.
* `requirements.txt` — dependencies.
* `account_sync.log` — generated log file.

---

## 📦 Requirements

```
requests
python-dotenv
```

---

## ✅ Best Practices

* Run with `--dry-run` first to preview changes.
* Use on test instances before production.
* Keep `.env` private and never commit it to Git.
* Check `account_sync.log` after each run.

---

© 2025 — Provided without warranty. Use responsibly.
