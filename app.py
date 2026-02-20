import os
import uuid
import threading
from flask import Flask, request, jsonify, send_file
from moviepy.editor import VideoFileClip, concatenate_videoclips

app = Flask(__name__)

UPLOAD_FOLDER = "uploads"
OUTPUT_FOLDER = "outputs"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

jobs = {}  # In-memory job storage


# -----------------------------
# Background merge
# -----------------------------
def merge_videos(job_id):
    job = jobs[job_id]
    job["status"] = "merging"
    try:
        clips = [VideoFileClip(path) for path in job["files"]]
        final = concatenate_videoclips(clips)
        output_path = os.path.join(OUTPUT_FOLDER, f"{job_id}.mp4")
        final.write_videofile(output_path)
        job["output"] = output_path
        job["status"] = "completed"
    except Exception as e:
        job["status"] = "failed"
        job["error"] = str(e)


# -----------------------------
# 1️⃣ Create Job
# -----------------------------
@app.route("/create-job", methods=["POST"])
def create_job():
    total_videos = int(request.form.get("total_videos", 0))
    if total_videos <= 0:
        return jsonify({"error": "Invalid total_videos"}), 400

    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        "total": total_videos,
        "received": 0,
        "files": [],
        "status": "waiting",
        "output": None
    }
    return jsonify({"job_id": job_id})


# -----------------------------
# 2️⃣ Upload video file
# -----------------------------
@app.route("/add-video/<job_id>", methods=["POST"])
def add_video(job_id):
    if job_id not in jobs:
        return jsonify({"error": "Job not found"}), 404

    job = jobs[job_id]

    if job["status"] not in ["waiting"]:
        return jsonify({"error": "Job already processing or finished"}), 400

    if "file" not in request.files:
        return jsonify({"error": "No file part"}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "No file selected"}), 400

    filename = os.path.join(UPLOAD_FOLDER, f"{job_id}_{job['received']}.mp4")
    file.save(filename)

    job["files"].append(filename)
    job["received"] += 1

    # Start merge automatically
    if job["received"] == job["total"]:
        job["status"] = "processing"
        threading.Thread(target=merge_videos, args=(job_id,)).start()

    return jsonify({
        "message": "Video received",
        "received": job["received"],
        "total": job["total"]
    })


# -----------------------------
# 3️⃣ Status
# -----------------------------
@app.route("/status/<job_id>", methods=["GET"])
def status(job_id):
    if job_id not in jobs:
        return jsonify({"error": "Job not found"}), 404
    job = jobs[job_id]
    return jsonify({
        "status": job["status"],
        "received": job["received"],
        "total": job["total"],
        "error": job.get("error")
    })


# -----------------------------
# 4️⃣ Download
# -----------------------------
@app.route("/download/<job_id>", methods=["GET"])
def download(job_id):
    if job_id not in jobs:
        return jsonify({"error": "Job not found"}), 404
    job = jobs[job_id]
    if job["status"] != "completed":
        return jsonify({"error": "Job not completed"}), 400
    return send_file(job["output"], as_attachment=True)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=port)
