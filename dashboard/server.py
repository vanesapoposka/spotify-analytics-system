"""Local dashboard server with demo login and personalized recommendations."""
import hashlib
import hmac
import errno
import json
import os
import secrets
import sys
import threading
import time
from http.cookies import SimpleCookie
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = Path(__file__).resolve().parent
ML_RESULTS = PROJECT_ROOT / "ml" / "ml_results.json"
sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import text
from etl.database_configuration import get_engine


ENGINE = get_engine()
SESSION_TTL_SECONDS = 60 * 60 * 8
SESSION_COOKIE = "spotify_analytics_session"
DEMO_PASSWORD = os.getenv("SPOTIFY_DEMO_PASSWORD", "password123")
_password_salt = secrets.token_bytes(16)
_password_hash = hashlib.pbkdf2_hmac("sha256", DEMO_PASSWORD.encode(), _password_salt, 180_000)
_sessions = {}
_sessions_lock = threading.Lock()


class ExclusiveThreadingHTTPServer(ThreadingHTTPServer):
    # Windows permits multiple listeners on one port when SO_REUSEADDR is set.
    # Keep this app on a unique port so an older static server cannot randomly
    # receive its requests and return HTML where the UI expects JSON.
    allow_reuse_address = False


def _session_for(handler):
    cookie = SimpleCookie()
    try:
        cookie.load(handler.headers.get("Cookie", ""))
        token = cookie[SESSION_COOKIE].value
    except (KeyError, TypeError):
        return None
    now = time.time()
    with _sessions_lock:
        session = _sessions.get(token)
        if not session or session["expires"] < now:
            _sessions.pop(token, None)
            return None
        session["expires"] = now + SESSION_TTL_SECONDS
        return {"username": session["username"], "user_sk": session["user_sk"]}


class DashboardHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(DASHBOARD_DIR), **kwargs)

    def end_headers(self):
        if urlsplit(self.path).path.endswith(".html"):
            self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def _json(self, payload, status=200, extra_headers=None):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        if extra_headers:
            for name, value in extra_headers.items():
                self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def _request_json(self):
        try:
            size = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            return None
        if size < 1 or size > 16_384:
            return None
        try:
            return json.loads(self.rfile.read(size))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None

    def do_GET(self):
        path = urlsplit(self.path).path
        if path == "/api/ml-results.json":
            if not ML_RESULTS.is_file():
                self.send_error(404, "ML results not found. Run python ml/run_ml.py first.")
                return
            payload = ML_RESULTS.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(payload)
            return
        if path == "/api/session":
            session = _session_for(self)
            self._json({"authenticated": bool(session),
                        "username": session["username"] if session else None})
            return
        if path == "/api/recommendations":
            session = _session_for(self)
            if not session:
                self._json({"error": "Log in to view personalized recommendations."}, 401)
                return
            try:
                from ml.user_recommender import get_recommendations
                self._json(get_recommendations(session["user_sk"]))
            except Exception as error:
                self.log_error("recommendation request failed: %s", error)
                self._json({"error": "Recommendations could not be loaded. Confirm the warehouse has listening data."}, 500)
            return
        super().do_GET()

    def do_POST(self):
        path = urlsplit(self.path).path
        body = self._request_json()
        if path == "/api/login":
            if not isinstance(body, dict):
                self._json({"error": "Send a username and password."}, 400)
                return
            username = str(body.get("username", "")).strip().upper()
            password = str(body.get("password", ""))
            if not username or len(username) > 64 or not password or len(password) > 1024:
                self._json({"error": "Enter a valid username and password."}, 400)
                return
            submitted_hash = hashlib.pbkdf2_hmac("sha256", password.encode(), _password_salt, 180_000)
            if not hmac.compare_digest(submitted_hash, _password_hash):
                self._json({"error": "That username or password is incorrect."}, 401)
                return
            try:
                with ENGINE.begin() as connection:
                    user = connection.execute(text("""
                        SELECT user_sk, user_bk FROM dwh.dim_user
                        WHERE user_bk = :username AND is_current = TRUE
                        ORDER BY version_no DESC LIMIT 1
                    """), {"username": username}).mappings().first()
            except Exception as error:
                self.log_error("warehouse login lookup failed: %s", error)
                self._json({"error": "Could not reach the warehouse. Check Docker/Postgres and .env."}, 503)
                return
            if not user:
                self._json({"error": "That username or password is incorrect."}, 401)
                return
            token = secrets.token_urlsafe(32)
            with _sessions_lock:
                _sessions[token] = {"username": user["user_bk"], "user_sk": int(user["user_sk"]),
                                    "expires": time.time() + SESSION_TTL_SECONDS}
            self._json({"authenticated": True, "username": user["user_bk"]}, extra_headers={
                "Set-Cookie": f"{SESSION_COOKIE}={token}; Path=/; HttpOnly; SameSite=Lax; Max-Age={SESSION_TTL_SECONDS}"
            })
            return
        if path == "/api/logout":
            cookie = SimpleCookie()
            cookie.load(self.headers.get("Cookie", ""))
            token = cookie[SESSION_COOKIE].value if SESSION_COOKIE in cookie else None
            if token:
                with _sessions_lock:
                    _sessions.pop(token, None)
            self._json({"authenticated": False}, extra_headers={
                "Set-Cookie": f"{SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0"
            })
            return
        self.send_error(404, "Unknown API endpoint")


if __name__ == "__main__":
    preferred_port = int(os.getenv("DASHBOARD_PORT", "8000"))
    server = None
    for port in range(preferred_port, preferred_port + 10):
        try:
            server = ExclusiveThreadingHTTPServer(("127.0.0.1", port), DashboardHandler)
            break
        except OSError as error:
            if error.errno not in (errno.EADDRINUSE, 10048):
                raise
    if server is None:
        raise RuntimeError(f"No free dashboard port found from {preferred_port} to {preferred_port + 9}.")
    address = server.server_address
    print(f"Serving Spotify dashboard at http://127.0.0.1:{address[1]}/spotify_dashboard.html")
    if address[1] != preferred_port:
        print(f"Port {preferred_port} was already in use; use the URL above for the updated login-enabled dashboard.")
    print("Demo accounts use existing warehouse usernames U000000–U003999.")
    print("Press Ctrl+C to stop the local server.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nDashboard server stopped.")
    finally:
        server.server_close()
