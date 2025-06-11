from flask import Flask, request, jsonify
import subprocess
import time

app = Flask(__name__)

@app.route('/submit', methods=['POST'])
def submit_job():
    try:
        start = time.time()
        subprocess.run(["spark-submit", "rest_job.py"], check=True)
        duration = time.time() - start
        return jsonify({"status": "success", "duration_sec": round(duration, 2)})
    except subprocess.CalledProcessError:
        return jsonify({"status": "failed"}), 500

if __name__ == '__main__':
    app.run(port=5050)
