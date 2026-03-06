import os
import uuid
import threading
import time
from flask import Flask, request, jsonify, send_file
from moviepy.editor import VideoFileClip, concatenate_videoclips

app = Flask(__name__)

UPLOAD_FOLDER = "uploads"
OUTPUT_FOLDER = "outputs"
JOB_EXPIRY_SECONDS = 2 * 60 * 60  # 2 hours

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

jobs = {}  # In-memory job storage
jobs_lock = threading.Lock()


# -----------------------------
# Helpers
# -----------------------------

def delete_file_safe(path):
    """Delete a file silently if it exists."""
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


def cleanup_job(job_id):
    """
    Delete all uploaded part files and the merged output file for a job,
    then remove the job from memory.
    """
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
    Background thread: wait JOB_EXPIRY_SECONDS, then if the job never
    received all videos (still stuck), clean it up.
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
            return  # completed or already cleaned up — do nothing

    for path in files_to_delete:
        delete_file_safe(path)
    delete_file_safe(output_to_delete)


def merge_videos(job_id):
    with jobs_lock:
        job = jobs.get(job_id)
        if not job:
            return
        job["status"] = "merging"
        files_snapshot = list(job["files"])

    try:
        clips = [VideoFileClip(path) for path in files_snapshot]
        final = concatenate_videoclips(clips)
        output_path = os.path.join(OUTPUT_FOLDER, f"{job_id}.mp4")
        final.write_videofile(output_path, logger=None)

        # Close clips to release file handles before deleting parts
        for clip in clips:
            clip.close()
        final.close()

        # Delete uploaded parts now that merge is done
        for path in files_snapshot:
            delete_file_safe(path)

        with jobs_lock:
            job = jobs.get(job_id)
            if job:
                job["output"] = output_path
                job["status"] = "completed"
                job["files"] = []  # Already deleted above

    except Exception as e:
        with jobs_lock:
            job = jobs.get(job_id)
            if job:
                job["status"] = "failed"
                job["error"] = str(e)


# -----------------------------
# 1. Create Job
# -----------------------------

@app.route("/create-job", methods=["POST"])
def create_job():
    total_videos = int(request.form.get("total_videos", 0))
    if total_videos <= 0:
        return jsonify({"error": "Invalid total_videos"}), 400

    job_id = str(uuid.uuid4())

    with jobs_lock:
        jobs[job_id] = {
            "total": total_videos,
            "received": 0,
            "files": [],
            # KEY FIX: "receiving" status accepts ALL part uploads.
            # Only transitions to "processing" once received == total.
            "status": "receiving",
            "output": None,
            "created_at": time.time()
        }

    # Start expiry watcher thread
    threading.Thread(
        target=expire_job_if_incomplete,
        args=(job_id,),
        daemon=True
    ).start()

    return jsonify({"job_id": job_id})


# -----------------------------
# 2. Upload Video Part
# -----------------------------

@app.route("/add-video/<job_id>", methods=["POST"])
def add_video(job_id):
    with jobs_lock:
        if job_id not in jobs:
            return jsonify({"error": "Job not found"}), 404

        job = jobs[job_id]

        # Accept uploads only while still receiving parts.
        # "processing" / "merging" / "completed" mean we already have all files.
        if job["status"] != "receiving":
            return jsonify({
                "error": f"Cannot upload to job with status '{job['status']}'"
            }), 400

        if "file" not in request.files:
            return jsonify({"error": "No file part"}), 400

        file = request.files["file"]
        if file.filename == "":
            return jsonify({"error": "No file selected"}), 400

        index = job["received"]
        filename = os.path.join(UPLOAD_FOLDER, f"{job_id}_{index}.mp4")
        file.save(filename)
        job["files"].append(filename)
        job["received"] += 1
        received = job["received"]
        total = job["total"]

        # Transition to "processing" only once ALL parts have arrived
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
        "message": "Video received",
        "received": received,
        "total": total
    })


# -----------------------------
# 3. Status
# -----------------------------

@app.route("/status/<job_id>", methods=["POST"])
def status(job_id):
    with jobs_lock:
        if job_id not in jobs:
            return jsonify({"error": "Job not found"}), 404
        job = jobs[job_id]
        return jsonify({
            "status": job["status"],
            "received": job["received"],
            "total": job["total"],
            "error": job.get("error"),
            "age_seconds": int(time.time() - job.get("created_at", time.time()))
        })


# -----------------------------
# 4. Download Merged Video
# -----------------------------

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

    # Clean up everything once the file is fully streamed back
    @response.call_on_close
    def do_cleanup():
        cleanup_job(job_id)

    return response


# -----------------------------
# Run
# -----------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
