import os
from functools import wraps
from typing import Any, cast, TypeVar, Callable, TYPE_CHECKING

import requests
from airflow.utils.airflow_flask_app import get_airflow_app
from flask import request, Response, make_response
from flask_login import login_user

if TYPE_CHECKING:
    pass

INTERNAL_GATEWAY_URL = os.getenv(
    "INTERNAL_GATEWAY_URL", "http://internal-gateway-service.cloud-core:8080"
)

REALM_AIRFLOW_USER = os.getenv("REALM_AIRFLOW_USER", "cloud-common")

T = TypeVar("T", bound=Callable)


def auth_current_user() -> dict[str, str | list[str] | Any] | None | Any:
    """Authenticate and set current user if Authorization header exists."""
    try:
        token = str(request.headers.get("Authorization")).split()[1]
        print(f"API TOKEN: {token}")
        headers = {"accept": "*/*", "Content-Type": "application/x-www-form-urlencoded"}

        payload = f"token={token}"
        response = requests.post(
            f"{INTERNAL_GATEWAY_URL}"
            f"/auth/realms/{REALM_AIRFLOW_USER}/protocol/openid-connect/token/introspect",
            headers=headers,
            data=payload,
        )

        if response.ok:
            user_info = response.json()
            userinfo = {
                "username": user_info["username"],
                "email": f"{user_info['username']}@qubership.com",
                "first_name": user_info["username"],
                "last_name": user_info["username"],
                "role_keys": ["airflow_admin"],
            }
            ab_security_manager = get_airflow_app().appbuilder.sm
            user = ab_security_manager.auth_user_oauth(userinfo)
            if user is not None:
                login_user(user, remember=False)
            return user
        else:
            return None
    except Exception:
        print("An error occurred during authorization, access is denied")
        return None


def init_app(_):
    """Initializes authentication backend"""


def requires_authentication(function: T):
    """Decorator for functions that require authentication"""

    @wraps(function)
    def decorated(*args, **kwargs):
        if auth_current_user() is not None:
            print("Authorized API Request")
            response = function(*args, **kwargs)
            response = make_response(response)
            return response
        else:
            return Response("Unauthorized", 401, {"WWW-Authenticate": "Basic"})

    return cast(T, decorated)
