from flask import Flask, redirect, request
from flask_cors import CORS
import requests, os, json
from pathlib import Path
from user_data_pullers.strava_keys import CLIENT_ID, CLIENT_SECRET

app = Flask(__name__)
CORS(app)

REDIRECT_URI = "http://127.0.0.1:5000/exchange"

@app.route('/')
def authorize():
    return redirect(
        f"https://www.strava.com/oauth/authorize"
        f"?client_id={CLIENT_ID}"
        f"&response_type=code"
        f"&redirect_uri={REDIRECT_URI}"
        f"&approval_prompt=force"
        f"&scope=read,activity:read_all"
    )

@app.route('/exchange')
def exchange_token():
    code = request.args.get('code')
    if not code:
        return "Missing code", 400

    response = requests.post(
        url="https://www.strava.com/oauth/token",
        data={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "code": code,
            "grant_type": "authorization_code"
        }
    )

    if response.status_code != 200:
        return f"Failed to exchange token: {response.text}", 400

    data = response.json()
    athlete = data.get("athlete", {})
    athlete_id = athlete.get("id")

    if not athlete_id:
        return f"❌ Could not find athlete ID in response: {data}", 400

    # Build user-specific folder (create if missing, reuse if exists)
    user_dir = Path("users_data") / f"ID{athlete_id}"
    user_dir.mkdir(parents=True, exist_ok=True)

    # Always overwrite tokens.json
    tokens_path = user_dir / "tokens.json"
    with open(tokens_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    return f"""
    <h2>✅ Authorization Successful</h2>
    <p><b>User ID:</b> {athlete_id}</p>
    <p>Tokens saved to: <code>{tokens_path}</code></p>
    <p>If the folder or file already existed, it was reused/overwritten.</p>
    """

if __name__ == "__main__":
    app.run(debug=True, port=5000)
