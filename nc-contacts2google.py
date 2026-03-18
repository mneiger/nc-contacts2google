#!/usr/bin/env python3
"""
carddav_google_sync.py
======================
One-way sync: Nextcloud CardDAV  →  Google Contacts (People API)

Features
--------
- Fetches all vCards from a CardDAV addressbook via PROPFIND/GET
- Parses vCard 3.0/4.0 fields: names, phones, emails, org, addresses,
  notes, URLs, IM handles, birthday
- Uses the vCard UID as a stable, human-readable external key stored in
  a Google Contact "biography" tag, so the script survives renames and
  re-runs without creating duplicates
- Creates / updates / deletes contacts in batch (up to 200 per call)
- Supports multiple Google accounts via a config list
- Dry-run mode (--dry-run) to inspect changes without touching Google
- Structured JSON log output

Requirements
------------
    pip install requests vobject google-auth google-auth-oauthlib \
                google-api-python-client

OAuth tokens
------------
Each Google account needs its own token file.  Run with --auth <account>
once per account in a browser-capable session to complete the OAuth flow.

Usage
-----
    python carddav_google_sync.py               # sync all configured accounts
    python carddav_google_sync.py --auth alice  # (re-)authorize one account
    python carddav_google_sync.py --dry-run     # show planned changes, no writes
    python carddav_google_sync.py --account alice  # sync one account only
"""

import argparse
import json
import logging
import math
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
import vobject
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ──────────────────────────────────────────────────────────────────────────────
# Configuration  (edit this section or point to an external config.json)
# ──────────────────────────────────────────────────────────────────────────────

CONFIG = {
    # ── Nextcloud / CardDAV source ────────────────────────────────────────────
    "carddav": {
        "url":          "https://your-nextcloud.example.org/remote.php/dav/addressbooks/users/YOUR_USER/contacts/",
        "username":     "YOUR_NEXTCLOUD_USER",
        # Use an app-password (Settings → Security → Devices & sessions)
        "password":     "YOUR_APP_PASSWORD",
        "verify_ssl":   True,   # set False only for self-signed certs (not recommended)
    },

    # ── Google OAuth2 app credentials ─────────────────────────────────────────
    # Download from Google Cloud Console → APIs & Services → Credentials
    "google_oauth_client_file": "client_secret.json",

    # ── Google accounts to sync TO ────────────────────────────────────────────
    # Add one entry per account.  token_file is created automatically on first
    # --auth run and reused on subsequent runs.
    "google_accounts": [
        {
            "name":       "alice",
            "token_file": "token_alice.json",
        },
        {
            "name":       "bob",
            "token_file": "token_bob.json",
        },
        # Add more accounts here …
    ],

    # ── Sync behaviour ────────────────────────────────────────────────────────
    # Label/group name created in each Google account to hold synced contacts.
    # All managed contacts are placed in this group; contacts outside it are
    # never touched.
    "contact_group_name": "NextcloudSync",

    # If True, contacts deleted from Nextcloud will also be deleted in Google.
    "sync_deletes": True,

    # Seconds to sleep between batch API calls to stay within rate limits.
    "batch_sleep": 1.0,

    # Log file path (set to None to log to stdout only)
    "log_file": "sync.log",
}

CONFIG = {
    # ── Nextcloud / CardDAV source ────────────────────────────────────────────
    "carddav": {
        "url":          "https://nextcloud.beth-hillel.org/remote.php/dav/addressbooks/users/automate/bh-full/",
        "username":     "automate",
        # Use an app-password (Settings → Security → Devices & sessions)
        "password":     "FB4ix-zaRxb-i2YEj-yNeFQ-xPz5s",
        "verify_ssl":   True,   # set False only for self-signed certs (not recommended)
    },

    # ── Google OAuth2 app credentials ─────────────────────────────────────────
    # Download from Google Cloud Console → APIs & Services → Credentials
    "google_oauth_client_file": "client_secret.json",

    # ── Google accounts to sync TO ────────────────────────────────────────────
    # Add one entry per account.  token_file is created automatically on first
    # --auth run and reused on subsequent runs.
    "google_accounts": [
        {
            "name":       "rabbin.neiger",
            "token_file": "token_rabbin.neiger.json",
        },
#        {
#            "name":       "bob",
#            "token_file": "token_bob.json",
#        },
        # Add more accounts here …
    ],

    # ── Sync behaviour ────────────────────────────────────────────────────────
    # Label/group name created in each Google account to hold synced contacts.
    # All managed contacts are placed in this group; contacts outside it are
    # never touched.
    "contact_group_name": "NextcloudSync",

    # If True, contacts deleted from Nextcloud will also be deleted in Google.
    "sync_deletes": True,

    # Seconds to sleep between batch API calls to stay within rate limits.
    "batch_sleep": 5.0,

    # Log file path (set to None to log to stdout only)
    "log_file": "sync.log",
}


# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────

SCOPES = ["https://www.googleapis.com/auth/contacts"]

# We stash the Nextcloud UID inside the Google contact's biography field
# using this sentinel prefix so we can find it later.
UID_PREFIX = "NC_UID:"

BATCH_SIZE_CREATE = 200
BATCH_SIZE_UPDATE = 200
BATCH_SIZE_DELETE = 500
BATCH_SIZE_GET    = 200

# People API fields we read back from Google
PERSON_FIELDS = (
    "names,emailAddresses,phoneNumbers,organizations,addresses,"
    "birthdays,urls,biographies,userDefined,relations,imClients,nicknames"
)

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────

def setup_logging(log_file: Optional[str]) -> logging.Logger:
    logger = logging.getLogger("carddav_sync")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s  %(levelname)-7s  %(message)s")
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    if log_file:
        fh = logging.FileHandler(log_file)
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger


log: logging.Logger = logging.getLogger("carddav_sync")   # replaced in main()

# ──────────────────────────────────────────────────────────────────────────────
# CardDAV fetcher
# ──────────────────────────────────────────────────────────────────────────────

PROPFIND_BODY = """\
<?xml version="1.0" encoding="UTF-8"?>
<C:addressbook-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">
  <D:prop>
    <D:getetag/>
    <C:address-data/>
  </D:prop>
</C:addressbook-query>
"""


def fetch_carddav(cfg: dict) -> Dict[str, Tuple[str, str]]:
    """
    Return {uid: (etag, vcard_text)} for every vCard in the addressbook.
    Uses a single REPORT request (RFC 6352 §8.6) to fetch all data at once.
    Falls back to PROPFIND + individual GETs if the server does not support
    address-book-query REPORT.
    """
    url      = cfg["url"].rstrip("/") + "/"
    auth     = (cfg["username"], cfg["password"])
    verify   = cfg["verify_ssl"]
    headers  = {"Depth": "1", "Content-Type": "application/xml; charset=utf-8"}

    log.info("Fetching vCards from %s", url)

    resp = requests.request(
        "REPORT", url,
        auth=auth, verify=verify, headers=headers, data=PROPFIND_BODY,
        timeout=120,
    )

    if resp.status_code == 207:
        return _parse_multistatus(resp.text, url, auth, verify)

    # Fallback: PROPFIND to list hrefs, then GET each
    log.warning("REPORT returned %s, falling back to PROPFIND+GET", resp.status_code)
    return _propfind_then_get(url, auth, verify)


def _parse_multistatus(xml_text: str, base_url: str, auth, verify: bool) -> Dict[str, Tuple[str, str]]:
    """Parse a DAV multistatus response; fetch missing vcard bodies individually."""
    from xml.etree import ElementTree as ET

    NS = {
        "D": "DAV:",
        "C": "urn:ietf:params:xml:ns:carddav",
    }

    tree = ET.fromstring(xml_text)
    result: Dict[str, Tuple[str, str]] = {}

    for response in tree.findall("D:response", NS):
        href_el = response.find("D:href", NS)
        if href_el is None:
            continue
        href = href_el.text or ""
        if not href.endswith(".vcf"):
            continue

        etag_el = response.find(".//D:getetag", NS)
        etag    = (etag_el.text or "").strip('"') if etag_el is not None else ""

        data_el = response.find(".//C:address-data", NS)
        vcard_text = data_el.text if data_el is not None else None

        if not vcard_text:
            # Fetch individually
            full_url = href if href.startswith("http") else base_url.split("/remote.php")[0] + href
            r = requests.get(full_url, auth=auth, verify=verify, timeout=30)
            if r.status_code == 200:
                vcard_text = r.text
            else:
                log.warning("Could not fetch %s (%s)", href, r.status_code)
                continue

        uid = _extract_uid(vcard_text) or _uid_from_href(href)
        result[uid] = (etag, vcard_text)

    log.info("Fetched %d vCards", len(result))
    return result


def _propfind_then_get(url: str, auth, verify: bool) -> Dict[str, Tuple[str, str]]:
    """Simple PROPFIND to list .vcf hrefs, then GET each one."""
    headers = {"Depth": "1", "Content-Type": "application/xml"}
    body = '<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:prop><D:href/><D:getetag/></D:prop></D:propfind>'
    resp = requests.request("PROPFIND", url, auth=auth, verify=verify,
                            headers=headers, data=body, timeout=60)
    resp.raise_for_status()

    from xml.etree import ElementTree as ET
    NS = {"D": "DAV:"}
    tree = ET.fromstring(resp.text)
    result: Dict[str, Tuple[str, str]] = {}

    for response in tree.findall("D:response", NS):
        href_el = response.find("D:href", NS)
        if href_el is None or not (href_el.text or "").endswith(".vcf"):
            continue
        href = href_el.text
        full_url = href if href.startswith("http") else url.split("/remote.php")[0] + href
        etag_el = response.find(".//D:getetag", NS)
        etag = (etag_el.text or "").strip('"') if etag_el is not None else ""

        r = requests.get(full_url, auth=auth, verify=verify, timeout=30)
        if r.status_code == 200:
            uid = _extract_uid(r.text) or _uid_from_href(href)
            result[uid] = (etag, r.text)
        else:
            log.warning("GET %s → %s", full_url, r.status_code)

    log.info("Fetched %d vCards (PROPFIND+GET fallback)", len(result))
    return result


def _extract_uid(vcard_text: str) -> Optional[str]:
    m = re.search(r"^UID[;:][^\r\n]+", vcard_text, re.MULTILINE | re.IGNORECASE)
    if not m:
        return None
    val = m.group(0).split(":", 1)[-1].strip()
    # Strip param parts like UID;VALUE=TEXT:...
    if ":" in val:
        val = val.split(":")[-1].strip()
    return val or None


def _uid_from_href(href: str) -> str:
    return os.path.basename(href).replace(".vcf", "")


# ──────────────────────────────────────────────────────────────────────────────
# vCard → Google Person converter
# ──────────────────────────────────────────────────────────────────────────────

def vcard_to_person(uid: str, vcard_text: str) -> Dict[str, Any]:
    """Convert a vCard string to a Google People API Person dict."""
    try:
        vc = vobject.readOne(vcard_text)
    except Exception as exc:
        log.warning("Could not parse vCard uid=%s: %s", uid, exc)
        return {}

    person: Dict[str, Any] = {}

    # ── Name ──────────────────────────────────────────────────────────────────
    if hasattr(vc, "n"):
        n = vc.n.value
        name_obj: Dict[str, str] = {}
        if n.family:    name_obj["familyName"]  = str(n.family)
        if n.given:     name_obj["givenName"]   = str(n.given)
        if n.additional:name_obj["middleName"]  = str(n.additional)
        if n.prefix:    name_obj["honorificPrefix"] = str(n.prefix)
        if n.suffix:    name_obj["honorificSuffix"] = str(n.suffix)
        if name_obj:
            person["names"] = [name_obj]

    if hasattr(vc, "fn") and vc.fn.value:
        person.setdefault("names", [{}])
        person["names"][0]["displayName"] = str(vc.fn.value)
        if not person["names"][0].get("familyName") and not person["names"][0].get("givenName"):
            person["names"][0]["unstructuredName"] = str(vc.fn.value)

    # ── Nickname ──────────────────────────────────────────────────────────────
    if hasattr(vc, "nickname") and vc.nickname.value:
        person["nicknames"] = [{"value": str(vc.nickname.value)}]

    # ── Phones ────────────────────────────────────────────────────────────────
    phones = []
    for tel in getattr(vc, "tel_list", []):
        val = str(tel.value).strip()
        if not val:
            continue
        types = [t.lower() for t in (tel.params.get("TYPE") or ["other"])]
        gtype = _map_phone_type(types)
        phones.append({"value": val, "type": gtype})
    if phones:
        person["phoneNumbers"] = phones

    # ── Emails ────────────────────────────────────────────────────────────────
    emails = []
    for em in getattr(vc, "email_list", []):
        val = str(em.value).strip()
        if not val:
            continue
        types = [t.lower() for t in (em.params.get("TYPE") or ["other"])]
        gtype = _map_email_type(types)
        emails.append({"value": val, "type": gtype})
    if emails:
        person["emailAddresses"] = emails

    # ── Organisation ─────────────────────────────────────────────────────────
    if hasattr(vc, "org"):
        org_val = vc.org.value
        org_name = str(org_val[0]) if isinstance(org_val, (list, tuple)) and org_val else str(org_val)
        dept = str(org_val[1]) if isinstance(org_val, (list, tuple)) and len(org_val) > 1 else ""
        title = str(vc.title.value) if hasattr(vc, "title") else ""
        person["organizations"] = [{
            "name":       org_name,
            "department": dept,
            "title":      title,
            "type":       "work",
        }]
    elif hasattr(vc, "title"):
        person["organizations"] = [{"title": str(vc.title.value)}]

    # ── Addresses ─────────────────────────────────────────────────────────────
    addresses = []
    for adr in getattr(vc, "adr_list", []):
        a = adr.value
        types = [t.lower() for t in (adr.params.get("TYPE") or ["other"])]
        gtype = "home" if "home" in types else "work" if "work" in types else "other"
        addr_obj: Dict[str, str] = {"type": gtype}
        if a.street:   addr_obj["streetAddress"]  = str(a.street)
        if a.city:     addr_obj["city"]            = str(a.city)
        if a.region:   addr_obj["region"]          = str(a.region)
        if a.code:     addr_obj["postalCode"]      = str(a.code)
        if a.country:  addr_obj["country"]         = str(a.country)
        if a.box:      addr_obj["poBox"]           = str(a.box)
        if any(addr_obj.get(k) for k in ("streetAddress","city","region","postalCode","country")):
            addresses.append(addr_obj)
    if addresses:
        person["addresses"] = addresses

    # ── Birthday ──────────────────────────────────────────────────────────────
    if hasattr(vc, "bday") and vc.bday.value:
        bday_str = str(vc.bday.value).replace("-", "")
        try:
            if len(bday_str) == 8:
                d = datetime.strptime(bday_str, "%Y%m%d")
                person["birthdays"] = [{"date": {"year": d.year, "month": d.month, "day": d.day}}]
            elif len(bday_str) == 4:  # --MMDD format (no year)
                d = datetime.strptime(bday_str[2:], "%m%d")
                person["birthdays"] = [{"date": {"month": d.month, "day": d.day}}]
        except ValueError:
            pass

    # ── URLs ──────────────────────────────────────────────────────────────────
    urls = []
    for url_prop in getattr(vc, "url_list", []):
        val = str(url_prop.value).strip()
        if val:
            urls.append({"value": val, "type": "homePage"})
    if urls:
        person["urls"] = urls

    # ── Note ─────────────────────────────────────────────────────────────────
    # Google allows exactly ONE biography entry. Merge NOTE + UID tag into one.
    note_text = str(vc.note.value).strip() if hasattr(vc, "note") and vc.note.value else ""
    uid_tag   = f"{UID_PREFIX}{uid}"
    combined  = f"{note_text}\n{uid_tag}".strip() if note_text else uid_tag
    person["biographies"] = [{"value": combined, "contentType": "TEXT_PLAIN"}]    

    return person


def _map_phone_type(types: List[str]) -> str:
    if "cell" in types or "mobile" in types: return "mobile"
    if "work" in types:  return "work"
    if "home" in types:  return "home"
    if "fax"  in types:  return "workFax"
    if "pager" in types: return "pager"
    return "other"


def _map_email_type(types: List[str]) -> str:
    if "work" in types:  return "work"
    if "home" in types:  return "home"
    return "other"


# ──────────────────────────────────────────────────────────────────────────────
# Google OAuth
# ──────────────────────────────────────────────────────────────────────────────

def get_google_service(account: dict, client_file: str):
    """Return an authenticated People API service for one account."""
    token_path = account["token_file"]
    creds: Optional[Credentials] = None

    if os.path.exists(token_path):
        try:
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)
            log.debug("[%s] Token loaded. valid=%s expired=%s has_refresh=%s has_token=%s",
                      account["name"], creds.valid, creds.expired,
                      bool(creds.refresh_token), bool(creds.token))
        except Exception as exc:
            log.error("[%s] Failed to load token file %s: %s", account["name"], token_path, exc)
            creds = None
    else:
        log.error("[%s] Token file not found: %s", account["name"], token_path)

    if not creds or not creds.valid:
        if creds and creds.refresh_token and (creds.expired or not creds.token):
            log.info("[%s] Refreshing OAuth token", account["name"])
            try:
                creds.refresh(Request())
                _save_token(creds, token_path)
            except Exception as exc:
                log.error("[%s] Token refresh failed: %s", account["name"], exc)
                raise RuntimeError(
                    f"Token refresh failed for '{account['name']}': {exc}\n"
                    f"Re-run:  python {sys.argv[0]} --auth {account['name']}"
                )
        else:
            raise RuntimeError(
                f"No valid token for account '{account['name']}'. "
                f"valid={getattr(creds,'valid',None)} "
                f"expired={getattr(creds,'expired',None)} "
                f"has_refresh={bool(getattr(creds,'refresh_token',None))} "
                f"has_token={bool(getattr(creds,'token',None))}\n"
                f"Run:  python {sys.argv[0]} --auth {account['name']}"
            )

    _save_token(creds, token_path)
    return build("people", "v1", credentials=creds, cache_discovery=False)

def authorize_account_old(account: dict, client_file: str):
    """Interactive browser OAuth flow — run once per account."""
    flow = InstalledAppFlow.from_client_secrets_file(client_file, SCOPES)
    creds = flow.run_local_server(port=0)
    _save_token(creds, account["token_file"])
    log.info("[%s] Authorization complete. Token saved to %s",
             account["name"], account["token_file"])


def authorize_account(account: dict, client_file: str):
    """Interactive browser OAuth flow — run once per account."""
    flow = InstalledAppFlow.from_client_secrets_file(client_file, SCOPES)
    creds = flow.run_local_server(port=0)

    # Force an immediate refresh so creds.token is populated
    # (right after OAuth flow, token may be None even with a valid refresh_token)
    if not creds.token:
        log.info("[%s] Fetching initial access token …", account["name"])
        creds.refresh(Request())

    _save_token(creds, account["token_file"])
    log.info("[%s] Authorization complete. Token saved to %s",
             account["name"], account["token_file"])
    

def _save_token(creds: Credentials, path: str):
    import json as _json
    token_data = {
        "token":         creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri":     creds.token_uri,
        "client_id":     creds.client_id,
        "client_secret": creds.client_secret,
        "scopes":        list(creds.scopes) if creds.scopes else [],
    }
    with open(path, "w") as f:
        _json.dump(token_data, f, indent=2)
    os.chmod(path, 0o600)   # lock permissions while we're at it    


# ──────────────────────────────────────────────────────────────────────────────
# Contact group helper
# ──────────────────────────────────────────────────────────────────────────────

def get_or_create_group(service, group_name: str) -> str:
    """Return the resource name of the named contact group, creating it if needed."""
    resp = service.contactGroups().list(pageSize=200).execute()
    for grp in resp.get("contactGroups", []):
        if grp.get("name") == group_name:
            return grp["resourceName"]

    result = service.contactGroups().create(body={"contactGroup": {"name": group_name}}).execute()
    log.info("Created contact group '%s' → %s", group_name, result["resourceName"])
    return result["resourceName"]


def assign_to_group(service, group_resource: str, member_resources: List[str]):
    """Add a list of contact resource names to a group (batches of 200)."""
    for i in range(0, len(member_resources), 200):
        chunk = member_resources[i:i + 200]
        service.contactGroups().members().modify(
            resourceName=group_resource,
            body={"resourceNamesToAdd": chunk},
        ).execute()
        time.sleep(0.5)


# ──────────────────────────────────────────────────────────────────────────────
# Google Contacts reader
# ──────────────────────────────────────────────────────────────────────────────

def fetch_google_contacts(service) -> Dict[str, Dict]:
    """
    Return {nc_uid: person_dict} for all Google contacts that contain our
    UID_PREFIX tag in their biographies.
    """
    all_contacts: Dict[str, Dict] = {}
    page_token = None

    log.info("Reading existing Google contacts …")
    while True:
        kwargs: Dict[str, Any] = {
            "resourceName": "people/me",
            "pageSize":     1000,
            "personFields": PERSON_FIELDS,
        }
        if page_token:
            kwargs["pageToken"] = page_token

        resp = service.people().connections().list(**kwargs).execute()

        for person in resp.get("connections", []):
            uid = _extract_nc_uid(person)
            if uid:
                all_contacts[uid] = person

        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    log.info("Found %d managed Google contacts (with NC_UID tag)", len(all_contacts))
    return all_contacts

def _extract_nc_uid(person: Dict) -> Optional[str]:
    for bio in person.get("biographies", []):
        for line in bio.get("value", "").splitlines():
            if line.startswith(UID_PREFIX):
                return line[len(UID_PREFIX):]
    return None

# ──────────────────────────────────────────────────────────────────────────────
# Diff / change detection
# ──────────────────────────────────────────────────────────────────────────────

def persons_differ(new_p: Dict, existing_p: Dict) -> bool:
    """
    Lightweight field-level diff.  Returns True if the new person data differs
    from what is already in Google (ignoring read-only metadata fields).
    """
    COMPARE_FIELDS = [
        "names", "phoneNumbers", "emailAddresses", "organizations",
        "addresses", "birthdays", "urls", "nicknames",
    ]
    for f in COMPARE_FIELDS:
        def normalise(lst):
            cleaned = []
            for item in (lst or []):
                item = {k: v for k, v in item.items()
                        if k not in ("metadata", "formattedValue", "formattedType",
                                     "canonicalForm", "displayName")}
                cleaned.append(item)
            return cleaned

        if normalise(new_p.get(f)) != normalise(existing_p.get(f)):
            return True

    # Compare notes except the UID line
    def bio_text(p):
        return [b["value"] for b in p.get("biographies", [])
                if not b.get("value", "").startswith(UID_PREFIX)]

    if bio_text(new_p) != bio_text(existing_p):
        return True

    return False


# ──────────────────────────────────────────────────────────────────────────────
# Core sync logic
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class SyncStats:
    created: int = 0
    updated: int = 0
    deleted: int = 0
    skipped: int = 0
    errors:  int = 0


def sync_to_account(
    account: dict,
    nc_contacts: Dict[str, Tuple[str, str]],
    cfg: dict,
    dry_run: bool,
) -> SyncStats:
    stats = SyncStats()
    name  = account["name"]
    log.info("── Syncing to account: %s ──", name)

    try:
        service = get_google_service(account, cfg["google_oauth_client_file"])
    except RuntimeError as exc:
        log.error(exc)
        stats.errors += 1
        return stats

    # ── Get or create the sync group ─────────────────────────────────────────
    group_resource = get_or_create_group(service, cfg["contact_group_name"])

    # ── Fetch existing managed contacts ──────────────────────────────────────
    existing: Dict[str, Dict] = fetch_google_contacts(service)

    # ── Build desired state from Nextcloud ───────────────────────────────────
    desired: Dict[str, Dict] = {}
    for uid, (etag, vcard_text) in nc_contacts.items():
        person = vcard_to_person(uid, vcard_text)
        if person:
            desired[uid] = person
        else:
            log.warning("Skipping uid=%s (parse error)", uid)
            stats.skipped += 1

    # ── Categorise ───────────────────────────────────────────────────────────
    to_create = {uid: p for uid, p in desired.items() if uid not in existing}
    to_delete = {uid: p for uid, p in existing.items() if uid not in desired and cfg["sync_deletes"]}
    to_check  = {uid: p for uid, p in desired.items() if uid in existing}

    to_update: Dict[str, Tuple[Dict, Dict]] = {}   # uid → (new_person, existing_person)
    for uid, new_p in to_check.items():
        ex_p = existing[uid]
        if persons_differ(new_p, ex_p):
            to_update[uid] = (new_p, ex_p)
        else:
            stats.skipped += 1

    log.info("[%s] Plan: +%d create  ~%d update  -%d delete  =%d skip",
             name, len(to_create), len(to_update), len(to_delete), stats.skipped)

    if dry_run:
        log.info("[%s] DRY-RUN – no changes written", name)
        stats.created = len(to_create)
        stats.updated = len(to_update)
        stats.deleted = len(to_delete)
        return stats

    # ── Creates ──────────────────────────────────────────────────────────────
    new_resource_names: List[str] = []
    uids_create = list(to_create.keys())
    for i in range(0, len(uids_create), BATCH_SIZE_CREATE):
        chunk_uids = uids_create[i:i + BATCH_SIZE_CREATE]
        contacts_payload = [{"contactPerson": to_create[u]} for u in chunk_uids]
        retries = 0
        while retries < 5:
            try:
                resp = service.people().batchCreateContacts(body={
                    "contacts":  contacts_payload,
                    "readMask":  "names,biographies",
                }).execute()
                for cr in resp.get("createdPeople", []):
                    rn = cr.get("person", {}).get("resourceName", "")
                    if rn:
                        new_resource_names.append(rn)
                        stats.created += 1
                log.info("[%s] Created batch %d/%d (%d contacts)",
                         name, i // BATCH_SIZE_CREATE + 1,
                         math.ceil(len(uids_create) / BATCH_SIZE_CREATE),
                         len(chunk_uids))
                break  # success
            except HttpError as exc:
                if exc.resp.status == 429:
                    wait = cfg["batch_sleep"] * (2 ** retries) + 10
                    log.warning("[%s] Rate limited (429), waiting %.0fs before retry %d/5 …",
                                name, wait, retries + 1)
                    time.sleep(wait)
                    retries += 1
                else:
                    log.error("[%s] batchCreate error: %s", name, exc)
                    stats.errors += 1
                    break
        else:
            log.error("[%s] Gave up on batch after 5 retries (rate limit)", name)
            stats.errors += 1
        time.sleep(cfg["batch_sleep"])

    # Assign new contacts to sync group
    if new_resource_names:
        try:
            assign_to_group(service, group_resource, new_resource_names)
        except HttpError as exc:
            log.warning("[%s] Group assignment error: %s", name, exc)

# ── Updates ──────────────────────────────────────────────────────────────
    uids_update = list(to_update.keys())
    for i in range(0, len(uids_update), BATCH_SIZE_UPDATE):
        chunk_uids = uids_update[i:i + BATCH_SIZE_UPDATE]
        contacts_map: Dict[str, Any] = {}
        for uid in chunk_uids:
            new_p, ex_p = to_update[uid]
            merged = {**new_p, "etag": ex_p.get("etag", "")}
            contacts_map[ex_p["resourceName"]] = merged

        update_mask = (
            "names,phoneNumbers,emailAddresses,organizations,"
            "addresses,birthdays,urls,biographies,nicknames"
        )
        retries = 0
        while retries < 5:
            try:
                service.people().batchUpdateContacts(body={
                    "contacts":   contacts_map,
                    "updateMask": update_mask,
                    "readMask":   "names",
                }).execute()
                stats.updated += len(chunk_uids)
                log.info("[%s] Updated batch %d/%d (%d contacts)",
                         name, i // BATCH_SIZE_UPDATE + 1,
                         math.ceil(len(uids_update) / BATCH_SIZE_UPDATE),
                         len(chunk_uids))
                break  # success
            except HttpError as exc:
                if exc.resp.status in (429, 500, 502, 503):
                    wait = cfg["batch_sleep"] * (2 ** retries) + 10
                    log.warning("[%s] batchUpdate transient error %s, waiting %.0fs (retry %d/5) …",
                                name, exc.resp.status, wait, retries + 1)
                    time.sleep(wait)
                    retries += 1
                else:
                    log.error("[%s] batchUpdate error: %s", name, exc)
                    stats.errors += 1
                    break
        else:
            log.error("[%s] Gave up on update batch after 5 retries", name)
            stats.errors += 1
        time.sleep(cfg["batch_sleep"])

    # ── Deletes ──────────────────────────────────────────────────────────────
    resource_names_delete = [existing[uid]["resourceName"] for uid in to_delete]
    for i in range(0, len(resource_names_delete), BATCH_SIZE_DELETE):
        chunk = resource_names_delete[i:i + BATCH_SIZE_DELETE]
        try:
            service.people().batchDeleteContacts(
                body={"resourceNames": chunk}
            ).execute()
            stats.deleted += len(chunk)
        except HttpError as exc:
            log.error("[%s] batchDelete error: %s", name, exc)
            stats.errors += 1
        time.sleep(cfg["batch_sleep"])

    log.info("[%s] Done: +%d created  ~%d updated  -%d deleted  err=%d",
             name, stats.created, stats.updated, stats.deleted, stats.errors)
    return stats


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

def main():
    global log
    parser = argparse.ArgumentParser(description="Sync Nextcloud CardDAV → Google Contacts")
    parser.add_argument("--auth",    metavar="ACCOUNT", help="Authorise a Google account (browser required)")
    parser.add_argument("--account", metavar="ACCOUNT", help="Sync only this account name")
    parser.add_argument("--dry-run", action="store_true", help="Show planned changes without writing")
    parser.add_argument("--config",  metavar="FILE",    help="External JSON config file")
    args = parser.parse_args()

    cfg = CONFIG
    if args.config:
        with open(args.config) as f:
            cfg = json.load(f)

    log = setup_logging(cfg.get("log_file"))

    # ── Auth mode ─────────────────────────────────────────────────────────────
    if args.auth:
        acct = next((a for a in cfg["google_accounts"] if a["name"] == args.auth), None)
        if not acct:
            log.error("Unknown account '%s'", args.auth)
            sys.exit(1)
        authorize_account(acct, cfg["google_oauth_client_file"])
        return

    # ── Determine accounts to sync ────────────────────────────────────────────
    accounts = cfg["google_accounts"]
    if args.account:
        accounts = [a for a in accounts if a["name"] == args.account]
        if not accounts:
            log.error("Unknown account '%s'", args.account)
            sys.exit(1)

    # ── Fetch Nextcloud once ──────────────────────────────────────────────────
    nc_contacts = fetch_carddav(cfg["carddav"])
    if not nc_contacts:
        log.warning("No vCards fetched from Nextcloud — aborting")
        sys.exit(0)

    # ── Sync each Google account ──────────────────────────────────────────────
    total = SyncStats()
    for account in accounts:
        s = sync_to_account(account, nc_contacts, cfg, dry_run=args.dry_run)
        total.created += s.created
        total.updated += s.updated
        total.deleted += s.deleted
        total.skipped += s.skipped
        total.errors  += s.errors

    log.info("═══ Total across %d account(s): +%d  ~%d  -%d  skip=%d  err=%d ═══",
             len(accounts), total.created, total.updated,
             total.deleted, total.skipped, total.errors)

    if total.errors:
        sys.exit(1)


if __name__ == "__main__":
    main()