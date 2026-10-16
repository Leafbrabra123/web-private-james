"""Backend API for direct MP3/MP4 conversion and downloads.

Runs entirely within the local project environment.
Supports:
1. Converting local video files (MP4/WebM/etc.) to MP3 via FFmpeg.
2. Converting public videos from YouTube, TikTok, Facebook, and Instagram to MP3/MP4.
3. Extracting and queueing YouTube playlists for sequential downloading.
4. Serving frontend files directly so the user can open http://127.0.0.1:5000/ directly.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
import threading
import time
import uuid
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadError

BASE_DIR = Path(__file__).resolve().parent
FRONTEND_DIR = BASE_DIR.parent / "frontend"

app = Flask(__name__)
# Allow up to 500 MB for local video file uploads to convert to MP3
app.config["MAX_CONTENT_LENGTH"] = int(os.getenv("MAX_CONTENT_LENGTH", str(500 * 1024 * 1024)))

# Enable CORS for all origins so any local server port (5500, 8080, 5000, 3000) can access the API
CORS(app, resources={r"/api/*": {"origins": "*"}}, allow_headers=["*"], expose_headers=["Content-Disposition"])

MAX_FILE_BYTES = int(os.getenv("MAX_FILE_BYTES", str(500 * 1024 * 1024)))
MAX_DURATION_SECONDS = int(os.getenv("MAX_DURATION_SECONDS", "7200"))
JOB_TTL_SECONDS = int(os.getenv("JOB_TTL_SECONDS", "3600"))
MAX_JOBS_PER_IP_PER_10_MIN = int(os.getenv("MAX_JOBS_PER_IP_PER_10_MIN", "30"))
MAX_ACTIVE_JOBS = int(os.getenv("MAX_ACTIVE_JOBS", "6"))

ROOT_DIR = Path(os.getenv("DOWNLOAD_DIR", str(Path(tempfile.gettempdir()) / "direct-video-converter")))
ROOT_DIR.mkdir(parents=True, exist_ok=True)

jobs: dict[str, dict] = {}
jobs_lock = threading.RLock()
executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="media-converter")
ip_job_history: dict[str, deque] = defaultdict(deque)

ALLOWED_DOMAINS = (
    "youtube.com",
    "youtu.be",
    "youtube-nocookie.com",
    "tiktok.com",
    "vm.tiktok.com",
    "vt.tiktok.com",
    "facebook.com",
    "fb.watch",
    "instagram.com",
    "instagr.am",
)


def _get_ffmpeg_path() -> str:
    path = os.getenv("FFMPEG_BINARY") or shutil.which("ffmpeg")
    return path or "ffmpeg"


def _is_allowed_host(host: str) -> bool:
    host = (host or "").lower().rstrip(".")
    return any(host == domain or host.endswith("." + domain) for domain in ALLOWED_DOMAINS)


def _is_youtube_playlist(raw_url: str) -> bool:
    try:
        parsed = urlparse(raw_url)
        host = (parsed.hostname or "").lower()
        if not any(host == d or host.endswith("." + d) for d in ("youtube.com", "youtu.be")):
            return False
        query = parse_qs(parsed.query)
        return "list" in query or parsed.path == "/playlist"
    except Exception:
        return False


def _validate_url(raw_url: object) -> str:
    if not isinstance(raw_url, str):
        raise ValueError("กรุณาระบุลิงก์วิดีโอ")
    raw_url = raw_url.strip()
    if not raw_url or len(raw_url) > 2048:
        raise ValueError("ลิงก์ว่างหรือยาวเกินไป")

    parsed = urlparse(raw_url)
    if parsed.scheme.lower() != "https":
        raise ValueError("รองรับเฉพาะลิงก์ HTTPS")
    if parsed.username or parsed.password:
        raise ValueError("ลิงก์รูปแบบนี้ไม่รองรับ")
    if not _is_allowed_host(parsed.hostname or ""):
        raise ValueError("รองรับเฉพาะลิงก์จาก YouTube, TikTok, Facebook และ Instagram")

    # Reject a standalone /playlist link for single convert; playlists should use /api/playlist
    if (parsed.hostname or "").lower().endswith("youtube.com") and parsed.path == "/playlist":
        if not parse_qs(parsed.query).get("v"):
            raise ValueError("ลิงก์นี้เป็นเพลย์ลิสต์ กรุณาใช้ตัวเลือกดาวน์โหลดเพลย์ลิสต์")

    return raw_url


def _safe_text(value: object, max_length: int = 180) -> str:
    text = str(value or "media_download")
    text = re.sub(r"[<>\"'`\\/:*?\x00-\x1f|]+", "_", text)
    text = re.sub(r"\s+", " ", text).strip(" ._")
    if not text:
        text = "media_download"
    return text[:max_length]


def _cleanup_old_jobs() -> None:
    cutoff = time.time() - JOB_TTL_SECONDS
    with jobs_lock:
        stale_ids = [
            job_id
            for job_id, job in jobs.items()
            if job.get("created_at", time.time()) < cutoff
            and job.get("status") in ("done", "error")
        ]
        for job_id in stale_ids:
            job = jobs.pop(job_id, None)
            if job:
                shutil.rmtree(job.get("directory", ""), ignore_errors=True)


def _ip_rate_limited(ip: str) -> bool:
    now = time.time()
    history = ip_job_history[ip]
    while history and now - history[0] > 600:
        history.popleft()
    if len(history) >= MAX_JOBS_PER_IP_PER_10_MIN:
        return True
    history.append(now)
    return False


def _choose_media_file(job_dir: Path, requested_format: str, media_id: str) -> Path | None:
    wanted_suffix = ".mp3" if requested_format == "mp3" else ".mp4"
    candidates = [
        path for path in job_dir.iterdir()
        if path.is_file() and path.suffix.lower() == wanted_suffix and not path.name.endswith(".part")
    ]
    if candidates:
        return max(candidates, key=lambda item: item.stat().st_mtime)

    if requested_format == "mp4":
        video_candidates = [
            path for path in job_dir.iterdir()
            if path.is_file()
            and path.suffix.lower() in {".mkv", ".webm", ".mov", ".m4v", ".avi"}
            and not path.name.endswith((".part", ".ytdl"))
        ]
        if video_candidates:
            source = max(video_candidates, key=lambda item: item.stat().st_mtime)
            output = job_dir / f"{media_id or 'video'}.mp4"
            ffmpeg = _get_ffmpeg_path()
            command = [
                ffmpeg, "-hide_banner", "-loglevel", "error", "-y", "-i", str(source),
                "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
                "-c:a", "aac", "-b:a", "192k", "-movflags", "+faststart", str(output),
            ]
            subprocess.run(command, check=True, capture_output=True, timeout=1800)
            return output if output.exists() else None
    return None


def _run_conversion(job_id: str, url: str, media_format: str, quality: str) -> None:
    with jobs_lock:
        job = jobs.get(job_id)
        if not job:
            return
        job["status"] = "processing"
        job["progress"] = 5
        job["status_text"] = "กำลังเริ่มดาวน์โหลด..."

    job_dir = Path(job["directory"])
    job_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = job_dir / "tmp"
    temp_dir.mkdir(exist_ok=True)

    def progress_hook(data: dict) -> None:
        with jobs_lock:
            current = jobs.get(job_id)
            if not current:
                return
            status = data.get("status")
            if status == "downloading":
                downloaded = data.get("downloaded_bytes") or 0
                total = data.get("total_bytes") or data.get("total_bytes_estimate")
                if total:
                    progress = int(85 * min(1.0, downloaded / total))
                    current["progress"] = max(current.get("progress", 5), min(85, progress))
                    current["status_text"] = f"กำลังดาวน์โหลด {current['progress']}%"
                else:
                    current["progress"] = max(current.get("progress", 5), 15)
                    current["status_text"] = "กำลังดาวน์โหลดข้อมูล..."
            elif status == "finished":
                current["progress"] = 88
                current["status_text"] = "กำลังแปลงรหัสไฟล์ด้วย FFmpeg..."

    def match_filter(info: dict, *, incomplete: bool = False):
        duration = info.get("duration")
        if duration and duration > MAX_DURATION_SECONDS:
            return f"วิดีโอยาวเกินกำหนด ({MAX_DURATION_SECONDS // 60} นาที)"
        size = info.get("filesize") or info.get("filesize_approx")
        if size and size > MAX_FILE_BYTES:
            return "ไฟล์มีขนาดใหญ่เกินขีดจำกัดที่กำหนด"
        return None

    ffmpeg_bin = _get_ffmpeg_path()
    node_bin = shutil.which("node")

    options = {
        "outtmpl": str(job_dir / "%(id)s.%(ext)s"),
        "paths": {"home": str(job_dir), "temp": str(temp_dir)},
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "ignoreerrors": False,
        "socket_timeout": 30,
        "retries": 3,
        "fragment_retries": 3,
        "max_filesize": MAX_FILE_BYTES,
        "match_filter": match_filter,
        "progress_hooks": [progress_hook],
        "overwrites": True,
        "restrictfilenames": True,
        "ffmpeg_location": ffmpeg_bin,
    }

    if node_bin:
        options["js_runtimes"] = {"node": {}}

    if media_format == "mp3":
        options.update({
            "format": "bestaudio/best",
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": quality,
            }],
        })
    else:
        height = int(quality) if quality.isdigit() else 720
        options.update({
            "format": (
                f"bestvideo[height<={height}][ext=mp4]+bestaudio[ext=m4a]/"
                f"bestvideo[height<={height}]+bestaudio/"
                f"best[height<={height}][ext=mp4]/best[height<={height}]/best"
            ),
            "merge_output_format": "mp4",
        })

    try:
        with YoutubeDL(options) as ydl:
            info = ydl.extract_info(url, download=True)

        if not info:
            raise RuntimeError("ไม่พบข้อมูลคลิปหรือคลิปไม่สามารถดาวน์โหลดได้")

        # If a playlist was passed to single convert, yt-dlp might extract entries
        if info.get("_type") == "playlist" and info.get("entries"):
            entries = [e for e in info.get("entries", []) if e]
            if entries:
                info = entries[0]

        media_id = str(info.get("id") or "video")
        file_path = _choose_media_file(job_dir, media_format, media_id)
        if not file_path or not file_path.exists():
            raise RuntimeError("ไม่พบไฟล์ผลลัพธ์ ตรวจสอบว่าระบบมี FFmpeg ติดตั้งแล้ว")

        actual_size = file_path.stat().st_size
        if actual_size > MAX_FILE_BYTES:
            file_path.unlink(missing_ok=True)
            raise RuntimeError("ไฟล์ผลลัพธ์ใหญ่เกินขีดจำกัด 500 MB")

        title = _safe_text(info.get("title") or media_id)
        filename = f"{title}.{media_format}"
        with jobs_lock:
            current = jobs.get(job_id)
            if current:
                current.update({
                    "status": "done",
                    "progress": 100,
                    "status_text": "เสร็จสมบูรณ์ พร้อมดาวน์โหลด",
                    "title": title,
                    "filename": filename,
                    "path": str(file_path),
                    "size_bytes": actual_size,
                    "finished_at": time.time(),
                })
    except Exception as exc:
        message = str(exc) or "เกิดข้อผิดพลาดระหว่างดาวน์โหลดหรือแปลงไฟล์"
        message = re.sub(r"\s+", " ", message).strip()[:350]
        with jobs_lock:
            current = jobs.get(job_id)
            if current:
                current.update({
                    "status": "error",
                    "progress": 0,
                    "status_text": "ล้มเหลว",
                    "error": message,
                    "finished_at": time.time(),
                })


# =========================================================================
# API ENDPOINTS
# =========================================================================

@app.get("/api/health")
def health():
    return jsonify({
        "success": True,
        "service": "direct-video-converter",
        "status": "online",
        "ffmpeg": bool(shutil.which("ffmpeg") or os.getenv("FFMPEG_BINARY")),
        "node": bool(shutil.which("node")),
    })


@app.post("/api/convert")
def create_conversion():
    _cleanup_old_jobs()
    payload = request.get_json(silent=True) or {}

    try:
        url = _validate_url(payload.get("url"))
    except ValueError as exc:
        return jsonify({"success": False, "error": str(exc)}), 400

    media_format = str(payload.get("format", "")).lower()
    quality = str(payload.get("quality", "")).strip()
    if media_format not in {"mp3", "mp4"}:
        return jsonify({"success": False, "error": "รูปแบบไฟล์ต้องเป็น MP3 หรือ MP4"}), 400
    if media_format == "mp3" and quality not in {"128", "192", "320"}:
        quality = "192"
    if media_format == "mp4" and quality not in {"360", "480", "720", "1080"}:
        quality = "720"

    ip = request.remote_addr or "unknown"
    with jobs_lock:
        if _ip_rate_limited(ip):
            return jsonify({"success": False, "error": "ส่งคำขอถี่เกินไป กรุณารอสักครู่แล้วลองใหม่"}), 429
        active_jobs = sum(1 for item in jobs.values() if item.get("status") in {"queued", "processing"})
        if active_jobs >= MAX_ACTIVE_JOBS:
            return jsonify({"success": False, "error": "ขณะนี้มีงานแปลงไฟล์จำนวนมาก กรุณารอสักครู่"}), 429

        job_id = uuid.uuid4().hex
        job_dir = ROOT_DIR / job_id
        jobs[job_id] = {
            "id": job_id,
            "status": "queued",
            "progress": 0,
            "status_text": "อยู่ในคิวประมวลผล",
            "title": "กำลังเตรียมการดาวน์โหลด",
            "format": media_format,
            "quality": quality,
            "created_at": time.time(),
            "directory": str(job_dir),
        }

    executor.submit(_run_conversion, job_id, url, media_format, quality)
    return jsonify({"success": True, "job_id": job_id}), 202


@app.post("/api/playlist")
def extract_playlist():
    payload = request.get_json(silent=True) or {}
    raw_url = str(payload.get("url", "")).strip()
    if not raw_url:
        return jsonify({"success": False, "error": "กรุณาระบุลิงก์เพลย์ลิสต์"}), 400
    if not _is_youtube_playlist(raw_url):
        return jsonify({"success": False, "error": "รองรับเฉพาะลิงก์เพลย์ลิสต์จาก YouTube"}), 400

    node_bin = shutil.which("node")
    ydl_opts = {
        "extract_flat": True,
        "quiet": True,
        "no_warnings": True,
        "noplaylist": False,
        "socket_timeout": 20,
    }
    if node_bin:
        ydl_opts["js_runtimes"] = {"node": {}}

    try:
        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(raw_url, download=False)
        if not info:
            return jsonify({"success": False, "error": "ไม่สามารถดึงข้อมูลเพลย์ลิสต์ได้"}), 400

        raw_entries = info.get("entries") or []
        entries = []
        for idx, entry in enumerate(raw_entries):
            if not entry:
                continue
            entry_id = entry.get("id") or ""
            entry_title = entry.get("title") or f"คลิปที่ {idx + 1}"
            entry_url = entry.get("url")
            if not entry_url and entry_id:
                entry_url = f"https://www.youtube.com/watch?v={entry_id}"
            if entry_url:
                entries.append({
                    "id": entry_id,
                    "title": entry_title,
                    "url": entry_url,
                    "duration": entry.get("duration") or 0,
                })

        return jsonify({
            "success": True,
            "title": info.get("title") or "YouTube Playlist",
            "entries": entries,
            "count": len(entries),
        })
    except Exception as exc:
        msg = str(exc) or "ไม่สามารถดึงข้อมูลเพลย์ลิสต์ได้"
        return jsonify({"success": False, "error": msg[:300]}), 400


@app.post("/api/convert-file")
def convert_local_file():
    _cleanup_old_jobs()
    if "file" not in request.files:
        return jsonify({"success": False, "error": "ไม่พบไฟล์ที่อัปโหลด"}), 400
    uploaded_file = request.files["file"]
    if not uploaded_file.filename:
        return jsonify({"success": False, "error": "ชื่อไฟล์ว่างเปล่า"}), 400

    target_format = request.form.get("format", "mp3").lower()
    quality = request.form.get("quality", "192")
    if target_format not in {"mp3", "wav"}:
        target_format = "mp3"

    job_id = uuid.uuid4().hex
    job_dir = ROOT_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)

    input_filename = _safe_text(Path(uploaded_file.filename).stem)
    input_suffix = Path(uploaded_file.filename).suffix or ".mp4"
    temp_input = job_dir / f"input{input_suffix}"
    output_filename = f"{input_filename}.{target_format}"
    output_file = job_dir / output_filename

    uploaded_file.save(str(temp_input))

    ffmpeg = _get_ffmpeg_path()
    cmd = [
        ffmpeg, "-hide_banner", "-loglevel", "error", "-y",
        "-i", str(temp_input),
        "-vn",
    ]
    if target_format == "mp3":
        bitrate = quality if quality in {"128", "192", "320"} else "192"
        cmd.extend(["-c:a", "libmp3lame", "-b:a", f"{bitrate}k"])
    else:
        cmd.extend(["-c:a", "pcm_s16le"])
    cmd.append(str(output_file))

    try:
        subprocess.run(cmd, check=True, capture_output=True, timeout=600)
    except Exception as exc:
        shutil.rmtree(job_dir, ignore_errors=True)
        return jsonify({"success": False, "error": f"แปลงไฟล์ไม่สำเร็จด้วย FFmpeg: {str(exc)[:200]}"}), 500

    if not output_file.exists():
        shutil.rmtree(job_dir, ignore_errors=True)
        return jsonify({"success": False, "error": "ไม่พบไฟล์ผลลัพธ์หลังแปลง"}), 500

    with jobs_lock:
        jobs[job_id] = {
            "id": job_id,
            "status": "done",
            "progress": 100,
            "status_text": "เสร็จสิ้น",
            "title": input_filename,
            "filename": output_filename,
            "format": target_format,
            "path": str(output_file),
            "size_bytes": output_file.stat().st_size,
            "created_at": time.time(),
            "finished_at": time.time(),
            "directory": str(job_dir),
        }

    return jsonify({
        "success": True,
        "job_id": job_id,
        "filename": output_filename,
        "size_bytes": output_file.stat().st_size,
        "download_url": f"/api/download/{job_id}",
    })


@app.get("/api/jobs/<job_id>")
def get_job(job_id: str):
    _cleanup_old_jobs()
    with jobs_lock:
        job = jobs.get(job_id)
        if not job:
            return jsonify({"success": False, "error": "ไม่พบงานนี้ หรือไฟล์หมดอายุแล้ว"}), 404
        return jsonify({
            "success": True,
            "status": job.get("status"),
            "progress": job.get("progress", 0),
            "status_text": job.get("status_text", ""),
            "title": job.get("title"),
            "filename": job.get("filename"),
            "size_bytes": job.get("size_bytes"),
            "error": job.get("error"),
        })


@app.get("/api/download/<job_id>")
def download_result(job_id: str):
    with jobs_lock:
        job = jobs.get(job_id)
        if not job or job.get("status") != "done" or not job.get("path"):
            return jsonify({"success": False, "error": "ไฟล์ยังไม่พร้อม หรือไฟล์หมดอายุแล้ว"}), 404
        path = Path(job["path"])
        filename = job.get("filename") or path.name
        media_format = job.get("format")

    if not path.is_file():
        return jsonify({"success": False, "error": "ไม่พบไฟล์ผลลัพธ์บนเซิร์ฟเวอร์"}), 404

    mime_type = "audio/mpeg" if media_format == "mp3" else "video/mp4"
    return send_file(
        path,
        mimetype=mime_type,
        as_attachment=True,
        download_name=filename,
        conditional=True,
        max_age=0,
    )


# =========================================================================
# STATIC FRONTEND SERVING (Makes http://127.0.0.1:5000/ work as full web app)
# =========================================================================

@app.get("/")
def index():
    index_file = FRONTEND_DIR / "index.html"
    if index_file.is_file():
        return send_file(index_file)
    return jsonify({"success": True, "service": "direct-video-converter", "status": "online"})


@app.get("/<path:filename>")
def static_proxy(filename: str):
    target = FRONTEND_DIR / filename
    if target.is_file():
        return send_file(target)
    return jsonify({"error": "Not found"}), 404


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
