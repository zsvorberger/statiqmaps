from flask import Blueprint, render_template, request, redirect, url_for, session
from flask_login import login_user, logout_user, current_user
from flask_bcrypt import Bcrypt

from models.models_users import (
    row_to_user,
    get_user_by_email,
    get_user_by_id,
    create_user,
    record_login,
)

auth_bp = Blueprint("auth_bp", __name__)
bcrypt = Bcrypt()  # init later with app


def init_auth(app):
    bcrypt.init_app(app)


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("index"))

    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")

        row = get_user_by_email(email)
        if row and bcrypt.check_password_hash(row["password_hash"], password):
            record_login(row["id"])
            fresh = get_user_by_id(row["id"])
            user = row_to_user(fresh)
            login_user(user)
            session["user_id"] = str(row["id"])
            next_url = request.args.get("next")
            return redirect(next_url or url_for("index"))

        return render_template("login.html", error="Invalid email or password.")

    return render_template("login.html")


@auth_bp.route("/register", methods=["GET", "POST"])
def register():
    if current_user.is_authenticated:
        return redirect(url_for("index"))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")

        if not email or not password:
            return render_template("register.html", error="Email and password required.")

        # basic duplicate check
        if get_user_by_email(email):
            return render_template("register.html", error="Email already registered.")

        pw_hash = bcrypt.generate_password_hash(password).decode("utf-8")
        if not username:
            username = email.split("@")[0]

        create_user(username, email, pw_hash)
        return redirect(url_for("auth_bp.login"))

    return render_template("register.html")


@auth_bp.route("/logout")
def logout():
    logout_user()
    session.pop("user_id", None)
    session.pop("strava_connect_state", None)
    return redirect(url_for("auth_bp.login"))
