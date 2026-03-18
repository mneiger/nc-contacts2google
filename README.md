# nc-contacts2google
# CardDAV → Google Contacts Sync
One-way sync from Nextcloud (CardDAV) to one or several Google Workspace accounts
using the **Google People API**.

---

## How it works

```
Nextcloud CardDAV
   │  REPORT/PROPFIND + GET all vCards (RFC 6352)
   ▼
Python script
   │  parse vCard 3/4  →  Google Person dict
   │  diff vs. existing Google contacts
   ▼
Google People API  (batchCreate / batchUpdate / batchDelete)
   ├── alice@yourdomain.org
   ├── bob@yourdomain.org
   └── …
```

Every synced contact is tagged with its Nextcloud UID in an invisible
biography field (`NC_UID:<uid>`).  This lets the script detect creates,
updates, and deletes reliably across runs — no duplicates.

All managed contacts are collected in a dedicated Contact Group
(`NextcloudSync` by default) so they are easy to identify and the script
never touches manually created contacts.

---

## Prerequisites

| What | Version |
|------|---------|
| Linux Mint / Ubuntu server | 20.04+ |
| Python | 3.9+ |
| Nextcloud | any recent version |
| Google Workspace | any edition |

---

## Step 1 — Install Python dependencies

```bash
sudo apt install python3-pip python3-venv
python3 -m venv ~/venv/carddav-sync
source ~/venv/carddav-sync/bin/activate
pip install requests vobject \
            google-auth google-auth-oauthlib \
            google-api-python-client
```

---

## Step 2 — Create a Google Cloud project & OAuth credentials

1. Go to <https://console.cloud.google.com/> and create a new project
   (e.g. `nextcloud-contact-sync`).
2. **Enable the People API**
   → *APIs & Services → Library → "People API" → Enable*
3. **Configure the OAuth consent screen**
   → *APIs & Services → OAuth consent screen*
   - User type: **Internal** (your Workspace org only — no review needed)
   - App name: `Nextcloud Contact Sync`
   - Scopes: add `https://www.googleapis.com/auth/contacts`
4. **Create OAuth credentials**
   → *APIs & Services → Credentials → Create Credentials → OAuth client ID*
   - Application type: **Desktop app**
   - Name: `carddav-sync-desktop`
   - Download the JSON → save as `client_secret.json` in the script folder

---

## Step 3 — Configure the script

Edit the `CONFIG` dict at the top of `carddav_google_sync.py`:

```python
"carddav": {
    "url":      "https://nextcloud.yourorg.org/remote.php/dav/addressbooks/users/admin/contacts/",
    "username": "admin",
    "password": "YOUR_APP_PASSWORD",   # Nextcloud Settings → Security → App passwords
    "verify_ssl": True,
},

"google_oauth_client_file": "client_secret.json",

"google_accounts": [
    {"name": "alice", "token_file": "token_alice.json"},
    {"name": "bob",   "token_file": "token_bob.json"},
],

"contact_group_name": "NextcloudSync",
"sync_deletes": True,
```

> **Tip:** you can also put all config in a separate `config.json` file and
> pass it with `--config config.json`.

---

## Step 4 — Authorise each Google account (one-time, needs a browser)

Run this **once per account** on a machine with a web browser (can be your
laptop, then copy the resulting token file to the server):

```bash
# Activate venv if needed
source ~/venv/carddav-sync/bin/activate

python carddav_google_sync.py --auth alice
# A browser window opens → sign in as alice@yourorg.org → allow access
# token_alice.json is created

python carddav_google_sync.py --auth bob
# token_bob.json is created
```

Copy token files to the server if you authorised on a different machine:
```bash
scp token_alice.json token_bob.json yourserver:~/carddav-sync/
```

---

## Step 5 — Test with a dry-run

```bash
python carddav_google_sync.py --dry-run
```

Output shows what *would* be created / updated / deleted without writing
anything.

---

## Step 6 — Run the sync

```bash
python carddav_google_sync.py
```

Or for a single account:
```bash
python carddav_google_sync.py --account alice
```

---

## Step 7 — Schedule with cron

```bash
crontab -e
```

Add (runs every 4 hours, logs appended to `sync.log`):

```cron
0 */4 * * * cd /home/youruser/carddav-sync && \
  /home/youruser/venv/carddav-sync/bin/python carddav_google_sync.py \
  >> /home/youruser/carddav-sync/sync.log 2>&1
```

Or with systemd timer for more control — see below.

---

## Optional: systemd timer (more robust than cron)

Create `/etc/systemd/system/carddav-sync.service`:
```ini
[Unit]
Description=CardDAV → Google Contacts sync
After=network-online.target

[Service]
Type=oneshot
User=youruser
WorkingDirectory=/home/youruser/carddav-sync
ExecStart=/home/youruser/venv/carddav-sync/bin/python carddav_google_sync.py
StandardOutput=append:/home/youruser/carddav-sync/sync.log
StandardError=append:/home/youruser/carddav-sync/sync.log
```

Create `/etc/systemd/system/carddav-sync.timer`:
```ini
[Unit]
Description=Run CardDAV sync every 4 hours

[Timer]
OnBootSec=5min
OnUnitActiveSec=4h
Persistent=true

[Install]
WantedBy=timers.target
```

Enable:
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now carddav-sync.timer
sudo systemctl list-timers carddav-sync.timer   # verify
```

---

## File layout

```
carddav-sync/
├── carddav_google_sync.py   # main script
├── client_secret.json       # OAuth client credentials (keep private!)
├── token_alice.json         # per-account OAuth token (auto-created)
├── token_bob.json
├── config.json              # (optional) external config
└── sync.log                 # appended log
```

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `No valid token … run --auth` | Token expired or missing; re-run `--auth` |
| `SSL verify failed` | Set `verify_ssl: false` (or install your CA cert) |
| `REPORT returned 405` | Script falls back to PROPFIND+GET automatically |
| Duplicate contacts | Check contacts have unique UIDs in Nextcloud |
| `quotaExceeded` error | Increase `batch_sleep` in config (default 1 s) |
| Missing fields | Open an issue; add the field mapping in `vcard_to_person()` |

---

## Supported vCard fields

| vCard field | Google field |
|-------------|-------------|
| `FN` / `N` | Display name / structured name |
| `NICKNAME` | Nicknames |
| `TEL` | Phone numbers (type mapped) |
| `EMAIL` | Email addresses (type mapped) |
| `ORG` + `TITLE` | Organizations |
| `ADR` | Addresses (home/work/other) |
| `BDAY` | Birthday (with or without year) |
| `URL` | URLs |
| `NOTE` | Biographies / notes |
| `UID` | Used internally as stable key |
