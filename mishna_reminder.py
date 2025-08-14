#!/usr/bin/env python3
"""
mishnah_reminder.py

- Fetches (and caches) Mishnah structure from Sefaria.
- Computes which two mishnayot correspond to "today" given a START_DATE and START_LOCATION.
- Sends the day's two mishnayot to a Telegram chat via bot.

ENV / secrets expected:
- TELEGRAM_BOT_TOKEN (required)
- TELEGRAM_CHAT_ID (required)
- START_DATE (YYYY-MM-DD) (required)
- START_MASECHET (required)
- START_PEREK (int, required)
- START_MISHNA (int, required)
- FORCE_REFRESH (optional, "1" to force re-build index)
- CACHE_FILE (optional, default "mishnah_index.json")
"""

import os
import sys
import json
from datetime import datetime, date, timezone
import requests
from typing import List, Tuple

# --- config ---
SEFARIA_INDEX_URL = "https://www.sefaria.org/api/index/Mishnah"
SEFARIA_TEXT_URL = "https://www.sefaria.org/api/texts/{title}.{chapter}?context=0&commentary=0&pad=0"
CACHE_FILE_DEFAULT = "mishnah_index.json"

# --- helpers for Sefaria calls ---
def fetch_json(url: str, timeout=15):
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()

def build_index(cache_file: str = CACHE_FILE_DEFAULT, force_refresh: bool = False):
    """
    Build a structured index: list of masechtot; for each masechet list of chapters;
    for each chapter, the number of mishnayot and (preferably) hebrew+english titles.

    Result format:
    {
      "built_at": "2025-08-14T...Z",
      "masechtot": [
         {
           "name": "Berakhot",
           "heName": "ברכות",           # if available
           "chapters": [
              {"chapter": 1, "mishnayot": 3},
              {"chapter": 2, "mishnayot": 4},
              ...
           ]
         },
         ...
      ]
    }
    """
    if os.path.exists(cache_file) and (not force_refresh):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            print("Warning: failed to read cache; rebuilding index...", file=sys.stderr)

    print("Fetching Mishnah index from Sefaria...", file=sys.stderr)
    idx = fetch_json(SEFARIA_INDEX_URL)
    # Find list of masechtot inside idx
    # The structure can vary; we try common keys and fallback.
    # We expect idx['contents'] or idx['contents'] to be a list of dicts or strings.
    contents = None
    for k in ("contents", "child_sections", "sections", "contents_html"):
        if k in idx:
            contents = idx[k]
            break
    if contents is None:
        # try a fallback: maybe idx has 'title' and 'nodes'
        if "nodes" in idx:
            contents = idx["nodes"]
    if not contents:
        raise RuntimeError("Could not parse Sefaria index JSON (missing contents). Please check API or provide a static index.")

    masechtot = []
    # contents elements can be dicts with "title" and "heTitle", or strings
    for entry in contents:
        if isinstance(entry, dict):
            # some children may be nested; detect leaf entries with 'title' but also with 'sections' etc.
            name = entry.get("title") or entry.get("heTitle") or entry.get("name")
            he = entry.get("heTitle") or entry.get("title") if entry.get("heTitle") else None
            # skip if name is not set
            if not name:
                continue
            masechtot.append({"name": name, "heName": he, "entry": entry})
        elif isinstance(entry, str):
            masechtot.append({"name": entry, "heName": None, "entry": None})
        else:
            # unknown type - skip
            continue

    # For each masechet, find number of chapters and mishnayot per chapter by probing text API
    out_masechtot = []
    for m in masechtot:
        title = m["name"]  # e.g., "Berakhot"
        he_title = m.get("heName")
        print(f"Processing {title} ...", file=sys.stderr)
        chapters = []
        # We'll step chapters 1.. until a request fails (404) or returns empty text
        chap = 1
        while True:
            try:
                url = SEFARIA_TEXT_URL.format(title="Mishnah_" + title.replace(" ", "_"), chapter=chap)
                # In some cases Sefaria titles use underscore vs hyphen; replace spaces with underscores.
                resp = requests.get(url, timeout=10)
                if resp.status_code == 404:
                    break
                resp.raise_for_status()
                data = resp.json()
                # data.get('text') may be a list if the chapter text is returned as list of mishnayot
                text = data.get("text") or data.get("he") or data.get("heText") or data.get("heSegment")
                # If text is a list of strings (mishnayot), len(text) is number of mishnayot
                if isinstance(text, list) and len(text) > 0:
                    mish_count = len(text)
                    chapters.append({"chapter": chap, "mishnayot": mish_count})
                    chap += 1
                else:
                    # No text or empty → stop
                    break
            except requests.HTTPError as e:
                print(f"HTTP error when fetching {title} chapter {chap}: {e}", file=sys.stderr)
                break
            except Exception as e:
                print(f"Error when fetching {title} chapter {chap}: {e}", file=sys.stderr)
                break

        if len(chapters) == 0:
            # skip empty masechtot (defensive)
            print(f"Warning: no chapters found for {title}. Skipping.", file=sys.stderr)
            continue

        out_masechtot.append({"name": title, "heName": he_title, "chapters": chapters})

    index = {"built_at": datetime.utcnow().isoformat() + "Z", "masechtot": out_masechtot}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)
    print(f"Index built and saved to {cache_file}.", file=sys.stderr)
    return index

# --- flatten corpus into ordered list of (masechet, heName, perek, mishna) ---
def flatten_index(index):
    flat = []
    for masechet in index["masechtot"]:
        name = masechet["name"]
        he = masechet.get("heName")
        for ch in masechet["chapters"]:
            chap_num = int(ch["chapter"])
            mish_count = int(ch["mishnayot"])
            for mish in range(1, mish_count + 1):
                flat.append((name, he, chap_num, mish))
    return flat

# --- locate the absolute start index from a start location ---
def find_start_index(flat_list, start_masechet, start_perek, start_mishna):
    # match masechet case-insensitively by english or hebrew
    s = start_masechet.strip().lower()
    for idx, (ename, he, perek, mish) in enumerate(flat_list):
        if (ename and ename.strip().lower() == s) or (he and he.strip().lower() == s):
            if perek == int(start_perek) and mish == int(start_mishna):
                return idx
    # if exact match not found, try partial match on masechet name start
    for idx, (ename, he, perek, mish) in enumerate(flat_list):
        if ename and ename.strip().lower().startswith(s):
            if perek == int(start_perek) and mish == int(start_mishna):
                return idx
    raise ValueError(f"Start location not found in corpus: {start_masechet} {start_perek}:{start_mishna}")

# --- format message in Hebrew + English fallback ---
def format_mishnah_item(item):
    en, he, perek, mish = item
    # produce Hebrew-style line: "מסכת X, פרק Y, משנה Z"
    if he:
        return f"מסכת {he}, פרק {perek}, משנה {mish}"
    else:
        return f"{en}, perek {perek}, mishna {mish}"

# --- send message via Telegram ---
def send_telegram(token: str, chat_id: str, text: str):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    r = requests.post(url, data=payload, timeout=15)
    r.raise_for_status()
    return r.json()

# --- main entrypoint ---
def main():
    # Read environment variables (these are set as GitHub secrets in the workflow)
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    start_date_str = os.environ.get("START_DATE")
    start_masechet = os.environ.get("START_MASECHET")
    start_perek = os.environ.get("START_PEREK")
    start_mishna = os.environ.get("START_MISHNA")
    force_refresh = os.environ.get("FORCE_REFRESH", "0") == "1"
    cache_file = os.environ.get("CACHE_FILE", CACHE_FILE_DEFAULT)

    for var, val in [
        ("TELEGRAM_BOT_TOKEN", token),
        ("TELEGRAM_CHAT_ID", chat_id),
        ("START_DATE", start_date_str),
        ("START_MASECHET", start_masechet),
        ("START_PEREK", start_perek),
        ("START_MISHNA", start_mishna),
    ]:
        if not val:
            print(f"Error: Required environment variable {var} is not set.", file=sys.stderr)
            sys.exit(2)

    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    except Exception:
        print("Error: START_DATE must be in YYYY-MM-DD format.", file=sys.stderr)
        sys.exit(2)

    # Build / load index
    index = build_index(cache_file=cache_file, force_refresh=force_refresh)

    flat = flatten_index(index)
    if len(flat) == 0:
        print("Error: flattened Mishnah list is empty.", file=sys.stderr)
        sys.exit(1)

    # determine start index
    try:
        start_idx = find_start_index(flat, start_masechet, int(start_perek), int(start_mishna))
    except Exception as e:
        print(f"Error locating start position: {e}", file=sys.stderr)
        # helpful fallback: show first 20 entries to help user find exact names
        sample = "\n".join(f"{i+1}: {format_mishnah_item(x)}" for i, x in enumerate(flat[:40]))
        print("Sample of beginning of corpus (first 40 entries):\n" + sample, file=sys.stderr)
        sys.exit(2)

    # compute how many days passed since start_date (in local date)
    today = date.today()
    days_since = (today - start_date).days
    if days_since < 0:
        print(f"Start date {start_date} is in the future relative to today {today}.", file=sys.stderr)
        sys.exit(2)

    # compute absolute index
    total = len(flat)
    abs_idx_for_today = (start_idx + days_since * 2) % total
    abs_idx_next = (abs_idx_for_today + 1) % total

    item1 = flat[abs_idx_for_today]
    item2 = flat[abs_idx_next]

    # build message
    date_str = today.isoformat()
    msg_lines = [
        f"משניות ליום {date_str}:",
        format_mishnah_item(item1),
        format_mishnah_item(item2),
        "", 
        "מקור הנתונים: Sefaria (Mishnah)"
    ]
    message = "\n".join(msg_lines)

    print("Sending message:", file=sys.stderr)
    print(message, file=sys.stderr)

    # send
    try:
        resp = send_telegram(token, chat_id, message)
        print("Sent successfully:", resp, file=sys.stderr)
    except Exception as e:
        print(f"Failed to send telegram message: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
