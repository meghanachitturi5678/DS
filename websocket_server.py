from flask import Flask, request, jsonify
from flask_cors import CORS  # Import CORS
from flask import send_file
from client import Client
import os

app = Flask(__name__)
CORS(app)  # Enable CORS for the Flask app
CORS(app, resources={r"/*": {"origins": "*"}})
client = Client()

UPLOAD_FOLDER = 'tmp/uploads'  # Change this to your preferred path

@app.route('/storage_used', methods=['GET'])
def get_storage_used():
    """Calculate and return the total storage used in UPLOAD_FOLDER."""
    print("Calculating storage used...")
    try:
        total_size = 0
        for filename in os.listdir(UPLOAD_FOLDER):
            filepath = os.path.join(UPLOAD_FOLDER, filename)
            if os.path.isfile(filepath):
                total_size += os.path.getsize(filepath)
        
        return jsonify({"status": "success", "storageUsed": total_size})
    except Exception as e:
        print("Error calculating storage used:", e)
        return jsonify({"status": "failure", "message": str(e)}), 500
    
@app.route('/upload', methods=['POST'])
def upload_file():
    """Endpoint for file upload."""
    if 'file' not in request.files:
        return jsonify({"status": "error", "message": "No file part in the request"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"status": "error", "message": "No selected file"}), 400

    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    filepath = os.path.join(UPLOAD_FOLDER, file.filename)
    
    try:
        file.save(filepath)  # Save the uploaded file
        response = client.upload_file(filepath)  # Process the file in the client
        return jsonify(response)
    except Exception as e:
        print("Error saving file:", e)
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/download', methods=['POST'])
def download_file():
    data = request.get_json()
    filename = data.get('filename')
    
    filepath = os.path.join(UPLOAD_FOLDER, filename)
    if not os.path.isfile(filepath):
        return jsonify({"status": "error", "message": "File not found"}), 404

    try:
        # Send file content back to the client for download
        return send_file(filepath, as_attachment=True)
    except Exception as e:
        print(f"Error downloading file: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500



@app.route('/list_files', methods=['GET'])
def list_files():
    """HTTP endpoint to list files in the upload directory."""
    try:
        files = []
        for filename in os.listdir(UPLOAD_FOLDER):
            filepath = os.path.join(UPLOAD_FOLDER, filename)
            if os.path.isfile(filepath):
                files.append({
                    "name": filename,
                    "size": os.path.getsize(filepath),
                    "lastModified": os.path.getmtime(filepath)  # Unix timestamp of last modification
                })
        print("Retrieved file list (HTTP):", files)
        return jsonify({"status": "success", "files": files})
    except Exception as e:
        print("Error retrieving file list (HTTP):", e)
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/lease', methods=['POST'])
def lease_file():
    data = request.get_json()
    filename = data.get('filename')
    if not filename:
        return jsonify({"status": "failure", "message": "Filename not provided"}), 400
    
    # Call the Client lease function
    try:
        client.lease_file(filename)
        return jsonify({"status": "success", "message": f"Lease granted for {filename}"})
    except Exception as e:
        print("Error leasing file:", e)
        return jsonify({"status": "failure", "message": str(e)}), 500

@app.route('/unlease', methods=['POST'])
def unlease_file():
    data = request.get_json()
    filename = data.get('filename')
    if not filename:
        return jsonify({"status": "failure", "message": "Filename not provided"}), 400
    
    # Call the Client unlease function
    try:
        client.unlease_file(filename)
        return jsonify({"status": "success", "message": f"Lease released for {filename}"})
    except Exception as e:
        print("Error releasing lease:", e)
        return jsonify({"status": "failure", "message": str(e)}), 500


if __name__ == "__main__":
    app.run(port=7083)
