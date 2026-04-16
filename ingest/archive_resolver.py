"""
Islam Stories — Smart Archive.org resolver and downloader.

Instead of hardcoding download URLs that break when Archive.org reorganizes,
this module uses the metadata API to discover available files for any identifier
and picks the best format automatically.

Usage:
    from ingest.archive_resolver import resolve_and_download

    path = resolve_and_download("cu31924028754616", dest_dir="/tmp/sources")
    # Returns Path to the downloaded file, or None on failure.

It can also search Archive.org for an item by keyword if the identifier is unknown:
    results = search_archive("Baddeley Russian Conquest Caucasus")
"""

import os
import re
import time
import requests
from pathlib import Path
from urllib.parse import quote

METADATA_API = "https://archive.org/metadata"
DOWNLOAD_BASE = "https://archive.org/download"
SEARCH_API = "https://archive.org/advancedsearch.php"

USER_AGENT = "Mozilla/5.0 (compatible; IslamStoriesBot/1.0)"
HEADERS = {"User-Agent": USER_AGENT}

# Preferred formats in priority order.
# For text ingestion we strongly prefer djvu.txt (already extracted text),
# then fall back to PDF, then other formats.
TEXT_FORMAT_PRIORITY = [
    ("_djvu.txt",),                       # Pre-extracted DjVu text — best for ingestion
    (".txt", ".text"),                     # Plain text
    (".pdf",),                            # PDF — needs extraction
    (".djvu",),                           # DjVu — needs conversion
    (".epub",),                           # EPUB
    (".html", ".htm"),                    # HTML
]

# For raw download (OCR pipeline, etc) we prefer the richest format
DOCUMENT_FORMAT_PRIORITY = [
    (".pdf",),
    (".djvu",),
    (".epub",),
    ("_djvu.txt",),
    (".txt", ".text"),
    (".html", ".htm"),
]

# Files to always skip
SKIP_PATTERNS = [
    "__ia_thumb", "_meta.xml", "_files.xml", "_meta.sqlite",
    ".torrent", "_archive.torrent", "_itemimage.jpg",
    "__ia_thumb.jpg", "_thumb.jpg",
]


def _should_skip(filename):
    """Check if a file should be skipped (metadata, thumbnails, etc)."""
    lower = filename.lower()
    return any(pat in lower for pat in SKIP_PATTERNS)


def get_metadata(identifier, timeout=30):
    """Fetch Archive.org metadata for an identifier."""
    url = f"{METADATA_API}/{identifier}"
    try:
        r = requests.get(url, timeout=timeout, headers=HEADERS)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"    Metadata fetch failed for {identifier}: {e}")
        return None


def pick_best_file(files, priority=None, prefer_largest=True):
    """
    From a list of archive.org file entries, pick the best downloadable format.
    Returns the file dict, or None.
    """
    if priority is None:
        priority = TEXT_FORMAT_PRIORITY

    for extensions in priority:
        candidates = []
        for f in files:
            name = f.get("name", "")
            if _should_skip(name):
                continue
            lower = name.lower()
            if any(lower.endswith(ext) for ext in extensions):
                size = int(f.get("size", 0) or 0)
                candidates.append((size, f))

        if candidates:
            # Pick largest (most complete) or smallest depending on preference
            candidates.sort(key=lambda x: x[0], reverse=prefer_largest)
            return candidates[0][1]

    return None


def resolve_download_url(identifier, prefer_text=True):
    """
    Given an Archive.org identifier, resolve the best download URL.

    Returns (url, filename, size_bytes) or (None, None, None) on failure.
    """
    meta = get_metadata(identifier)
    if not meta:
        return None, None, None

    files = meta.get("files", [])
    if not files:
        print(f"    No files found for {identifier}")
        return None, None, None

    priority = TEXT_FORMAT_PRIORITY if prefer_text else DOCUMENT_FORMAT_PRIORITY
    best = pick_best_file(files, priority=priority)

    if not best:
        # Last resort: pick any non-metadata file
        for f in files:
            if not _should_skip(f.get("name", "")):
                best = f
                break

    if not best:
        print(f"    No suitable file found for {identifier}")
        return None, None, None

    filename = best["name"]
    url = f"{DOWNLOAD_BASE}/{identifier}/{quote(filename)}"
    size = int(best.get("size", 0) or 0)
    return url, filename, size


def resolve_and_download(identifier, dest_dir, label=None, prefer_text=True,
                         timeout=180, skip_existing=True):
    """
    Resolve the best file for an Archive.org identifier and download it.

    Args:
        identifier: Archive.org item identifier
        dest_dir: Directory to save the file
        label: Human-readable label (used for filename). If None, uses identifier.
        prefer_text: If True, prefer djvu.txt > txt > pdf. If False, prefer pdf > djvu > txt.
        timeout: Download timeout in seconds
        skip_existing: Skip if file already exists with non-zero size

    Returns:
        Path to downloaded file, or None on failure.
    """
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)

    url, remote_filename, size = resolve_download_url(identifier, prefer_text=prefer_text)
    if not url:
        return None

    # Build local filename
    ext = os.path.splitext(remote_filename)[1] or ".bin"
    # For djvu.txt, preserve the full extension
    if remote_filename.lower().endswith("_djvu.txt"):
        ext = ".txt"

    if label:
        safe_label = re.sub(r'[^\w\s\-()]', '_', label).strip()
        local_name = f"{safe_label}{ext}"
    else:
        local_name = f"{identifier}{ext}"

    dest_path = dest_dir / local_name

    if skip_existing and dest_path.exists() and dest_path.stat().st_size > 0:
        print(f"    SKIP (exists): {dest_path.name}")
        return dest_path

    size_mb = size / (1024 * 1024) if size else 0
    print(f"    Downloading: {remote_filename} ({size_mb:.1f} MB)")

    try:
        with requests.get(url, stream=True, timeout=timeout, headers=HEADERS) as resp:
            resp.raise_for_status()
            with open(dest_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=256 * 1024):
                    f.write(chunk)

        actual_mb = dest_path.stat().st_size / (1024 * 1024)
        print(f"    SAVED: {dest_path.name} ({actual_mb:.1f} MB)")
        return dest_path

    except Exception as e:
        print(f"    Download failed: {e}")
        if dest_path.exists():
            dest_path.unlink()
        return None


def resolve_text(identifier, dest_dir=None, timeout=180):
    """
    Convenience: resolve and download the best text file for an identifier.
    If it's a djvu.txt, returns the text content directly.
    If it's a PDF, downloads it for later extraction.

    Returns (text_content, file_path) — text_content is None if the file is a PDF.
    """
    if dest_dir is None:
        dest_dir = Path("/tmp/archive_downloads")

    path = resolve_and_download(identifier, dest_dir, prefer_text=True, timeout=timeout)
    if not path:
        return None, None

    if path.suffix.lower() == '.txt':
        text = path.read_text(encoding='utf-8', errors='replace')
        return text, path
    else:
        # PDF or other binary — caller needs to extract text
        return None, path


def download_djvu_text(identifier, filename, dest_dir):
    """
    Download a specific djvu.txt file from Archive.org.
    Falls back to metadata API resolution if the direct URL fails.

    Returns Path to the downloaded file, or None.
    """
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)

    local_path = dest_dir / f"{identifier}.txt"
    if local_path.exists() and local_path.stat().st_size > 1000:
        return local_path

    # Try direct URL first
    url = f"{DOWNLOAD_BASE}/{identifier}/{quote(filename)}"
    try:
        r = requests.get(url, timeout=180, stream=True, headers=HEADERS)
        r.raise_for_status()
        with open(local_path, 'wb') as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
        if local_path.stat().st_size > 100:
            return local_path
    except Exception:
        pass

    # Fallback: let the resolver find the best text file
    print(f"    Direct URL failed, trying metadata API...")
    path = resolve_and_download(identifier, dest_dir, prefer_text=True)
    return path


def search_archive(query, rows=5):
    """
    Search Archive.org for items matching a query.
    Returns a list of (identifier, title, description) tuples.
    """
    params = {
        "q": query,
        "fl[]": ["identifier", "title", "description"],
        "rows": rows,
        "output": "json",
    }
    try:
        r = requests.get(SEARCH_API, params=params, timeout=15, headers=HEADERS)
        r.raise_for_status()
        docs = r.json().get("response", {}).get("docs", [])
        results = []
        for doc in docs:
            results.append((
                doc.get("identifier", ""),
                doc.get("title", ""),
                doc.get("description", "")[:200] if doc.get("description") else "",
            ))
        return results
    except Exception as e:
        print(f"    Search failed: {e}")
        return []


def try_identifiers(identifiers, dest_dir, prefer_text=True, label=None):
    """
    Try multiple Archive.org identifiers until one succeeds.
    Useful when the same text exists under different identifiers.

    Returns Path to downloaded file, or None if all fail.
    """
    for ident in identifiers:
        print(f"    Trying identifier: {ident}")
        path = resolve_and_download(ident, dest_dir, label=label, prefer_text=prefer_text)
        if path:
            return path
        time.sleep(0.5)
    return None
