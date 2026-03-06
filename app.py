import os
import uuid
import hashlib
import threading
import time
import glob
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from moviepy.editor import VideoFileClip, concatenate_videoclips

app = Flask(__name__)
CORS(app)  # Allow dashboard to call the API from any origin

UPLOAD_FOLDER = "uploads"
OUTPUT_FOLDER = "outputs"
JOB_EXPIRY_SECONDS = 2 * 60 * 60  # 2 hours

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

jobs = {}
jobs_lock = threading.Lock()


# ─────────────────────────────────────────
# Startup: purge any leftover files from
# a previous run (crash, restart, etc.)
# ─────────────────────────────────────────

def purge_stale_files():
    """
    Called once at startup. Removes every file in uploads/ and outputs/
    that belongs to no currently-tracked job. Since jobs{} is empty on
    a fresh start, this wipes everything cleanly.
    """
    for folder in (UPLOAD_FOLDER, OUTPUT_FOLDER):
        for path in glob.glob(os.path.join(folder, "*")):
            try:
                os.remove(path)
            except Exception:
                pass


purge_stale_files()


# ─────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────

def delete_file_safe(path):
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


def cleanup_job(job_id):
    """Delete all files for a job and remove it from memory."""
    with jobs_lock:
        job = jobs.get(job_id)
        if not job:
            return
        files_to_delete = list(job.get("files", []))
        output_to_delete = job.get("output")
        del jobs[job_id]

    for path in files_to_delete:
        delete_file_safe(path)
    delete_file_safe(output_to_delete)


def expire_job_if_incomplete(job_id):
    """
    Background thread: after JOB_EXPIRY_SECONDS, if the job is still
    in an unfinished state, expire and clean it up.
    """
    time.sleep(JOB_EXPIRY_SECONDS)
    with jobs_lock:
        job = jobs.get(job_id)
        if not job:
            return
        if job["status"] in ("receiving", "processing", "merging"):
            files_to_delete = list(job.get("files", []))
            output_to_delete = job.get("output")
            del jobs[job_id]
        else:
            return

    for path in files_to_delete:
        delete_file_safe(path)
    delete_file_safe(output_to_delete)


def compute_md5(path, chunk_size=65536):
    """Return MD5 hex digest of a file."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def merge_videos(job_id):
    with jobs_lock:
        job = jobs.get(job_id)
        if not job:
            return
        job["status"] = "merging"
        job["merge_started_at"] = time.time()
        files_snapshot = list(job["files"])

    try:
        clips = [VideoFileClip(path) for path in files_snapshot]
        final = concatenate_videoclips(clips)
        output_path = os.path.join(OUTPUT_FOLDER, f"{job_id}.mp4")
        final.write_videofile(output_path, logger=None)

        for clip in clips:
            clip.close()
        final.close()

        for path in files_snapshot:
            delete_file_safe(path)

        with jobs_lock:
            job = jobs.get(job_id)
            if job:
                job["output"] = output_path
                job["status"] = "completed"
                job["completed_at"] = time.time()
                job["files"] = []

    except Exception as e:
        with jobs_lock:
            job = jobs.get(job_id)
            if job:
                job["status"] = "failed"
                job["error"] = str(e)


# ─────────────────────────────────────────
# 1. Create Job
# ─────────────────────────────────────────

@app.route("/create-job", methods=["POST"])
def create_job():
    total_videos = int(request.form.get("total_videos", 0))
    if total_videos <= 0:
        return jsonify({"error": "Invalid total_videos"}), 400

    job_id = str(uuid.uuid4())
    now = time.time()

    with jobs_lock:
        # Clean up any old completed/failed/expired jobs to free memory
        stale = [
            jid for jid, j in jobs.items()
            if j["status"] in ("completed", "failed")
            and now - j.get("created_at", now) > 300  # older than 5 min
        ]
        for jid in stale:
            files_to_del = list(jobs[jid].get("files", []))
            out_to_del = jobs[jid].get("output")
            del jobs[jid]
            for p in files_to_del:
                delete_file_safe(p)
            delete_file_safe(out_to_del)

        jobs[job_id] = {
            "total": total_videos,
            "received": 0,
            "files": [],
            "file_sizes": [],      # track expected vs actual byte sizes
            "file_checksums": [],  # md5 of each confirmed part
            "status": "receiving",
            "output": None,
            "error": None,
            "created_at": now,
            "completed_at": None,
            "merge_started_at": None,
            "parts_log": []        # per-part upload events for dashboard
        }

    threading.Thread(
        target=expire_job_if_incomplete,
        args=(job_id,),
        daemon=True
    ).start()

    return jsonify({"job_id": job_id})


# ─────────────────────────────────────────
# 2. Upload Video Part
# ─────────────────────────────────────────

@app.route("/add-video/<job_id>", methods=["POST"])
def add_video(job_id):
    with jobs_lock:
        if job_id not in jobs:
            return jsonify({"error": "Job not found"}), 404
        job = jobs[job_id]
        if job["status"] != "receiving":
            return jsonify({
                "error": f"Cannot upload to job with status '{job['status']}'"
            }), 400

    if "file" not in request.files:
        return jsonify({"error": "No file part"}), 400
    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "No file selected"}), 400

    # Read file bytes fully into memory first so we can verify integrity
    file_bytes = file.read()
    received_size = len(file_bytes)
    if received_size == 0:
        return jsonify({"error": "Uploaded file is empty"}), 400

    # Get the Content-Length the client declared (optional but used if present)
    declared_size = request.content_length  # may be None

    with jobs_lock:
        if job_id not in jobs:
            return jsonify({"error": "Job not found"}), 404
        job = jobs[job_id]
        if job["status"] != "receiving":
            return jsonify({
                "error": f"Cannot upload to job with status '{job['status']}'"
            }), 400

        index = job["received"]
        filename = os.path.join(UPLOAD_FOLDER, f"{job_id}_{index}.mp4")

        # Write to disk
        with open(filename, "wb") as f:
            f.write(file_bytes)

        # Verify: re-read size from disk to confirm write completed
        disk_size = os.path.getsize(filename)
        if disk_size != received_size:
            # Write was incomplete — delete and reject
            delete_file_safe(filename)
            return jsonify({
                "error": "File write verification failed",
                "expected_bytes": received_size,
                "written_bytes": disk_size
            }), 500

        # Compute checksum for this part
        checksum = compute_md5(filename)

        job["files"].append(filename)
        job["file_sizes"].append(disk_size)
        job["file_checksums"].append(checksum)
        job["received"] += 1
        job["parts_log"].append({
            "part": index + 1,
            "size_bytes": disk_size,
            "checksum": checksum,
            "uploaded_at": time.time()
        })

        received = job["received"]
        total = job["total"]

        if received == total:
            job["status"] = "processing"
            start_merge = True
        else:
            start_merge = False

    if start_merge:
        threading.Thread(
            target=merge_videos,
            args=(job_id,),
            daemon=True
        ).start()

    return jsonify({
        "message": "Video part received and verified",
        "part": index + 1,
        "received": received,
        "total": total,
        "size_bytes": disk_size,
        "checksum": checksum
    })


# ─────────────────────────────────────────
# 3. Status  (used by n8n and dashboard)
# ─────────────────────────────────────────

@app.route("/status/<job_id>", methods=["POST", "GET"])
def status(job_id):
    with jobs_lock:
        if job_id not in jobs:
            return jsonify({"error": "Job not found"}), 404
        job = jobs[job_id]
        now = time.time()
        return jsonify({
            "job_id": job_id,
            "status": job["status"],
            "received": job["received"],
            "total": job["total"],
            "parts_log": job.get("parts_log", []),
            "error": job.get("error"),
            "age_seconds": int(now - job.get("created_at", now)),
            "merge_elapsed_seconds": (
                int(now - job["merge_started_at"])
                if job.get("merge_started_at") else None
            )
        })


# ─────────────────────────────────────────
# 4. All Jobs  (dashboard overview)
# ─────────────────────────────────────────

@app.route("/jobs", methods=["GET"])
def list_jobs():
    with jobs_lock:
        now = time.time()
        result = []
        for jid, job in jobs.items():
            result.append({
                "job_id": jid,
                "status": job["status"],
                "received": job["received"],
                "total": job["total"],
                "age_seconds": int(now - job.get("created_at", now)),
                "error": job.get("error"),
                "merge_elapsed_seconds": (
                    int(now - job["merge_started_at"])
                    if job.get("merge_started_at") else None
                )
            })
        result.sort(key=lambda x: -x["age_seconds"])
        return jsonify(result)


# ─────────────────────────────────────────
# 5. Download Merged Video
# ─────────────────────────────────────────

@app.route("/download/<job_id>", methods=["GET"])
def download(job_id):
    with jobs_lock:
        if job_id not in jobs:
            return jsonify({"error": "Job not found"}), 404
        job = jobs[job_id]
        if job["status"] != "completed":
            return jsonify({
                "error": f"Job not completed (current status: {job['status']})"
            }), 400
        output_path = job["output"]

    if not output_path or not os.path.exists(output_path):
        return jsonify({"error": "Output file missing"}), 500

    response = send_file(
        output_path,
        mimetype="video/mp4",
        as_attachment=True,
        download_name=f"{job_id}.mp4"
    )

    @response.call_on_close
    def do_cleanup():
        cleanup_job(job_id)

    return response


# ─────────────────────────────────────────
# Run
# ─────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
