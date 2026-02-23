from flask import session
from flask_login import current_user

from models.models_users import get_user_by_id


def get_current_app_user_id():
    if current_user.is_authenticated:
        return str(current_user.id)
    return session.get("user_id")


def get_strava_id_for_user(app_user_id):
    if not app_user_id:
        return None
    row = get_user_by_id(app_user_id)
    if row and row["strava_athlete_id"]:
        return str(row["strava_athlete_id"])
    return None
