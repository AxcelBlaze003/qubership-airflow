import os
import logging
import jwt
import requests
from flask_appbuilder import expose
from flask_appbuilder.security.manager import AUTH_OAUTH
from flask_appbuilder.security.views import AuthOAuthView
from airflow.providers.fab.auth_manager.security_manager.override import (
    FabAirflowSecurityManagerOverride,
)
from airflow.configuration import conf

logging.basicConfig(
    format="[%(asctime)s] [%(levelname)s] [%(filename)s] [thread=%(threadName)s] %(message)s",
    level=logging.DEBUG,
)

REALM_ID = os.getenv("KEYCLOAK_REALM_ID", "8146a86c-8a77-4473-a8dc-ea9d15478920")

CLIENT_ID = os.getenv("CLIENT_ID", "testClient")

PRIVATE_GATEWAY_EXTERNAL_URL = os.getenv(
    "PRIVATE_GATEWAY_EXTERNAL_URL", "http://private-gateway.k8saddress.qubership.com"
)

PRIVATE_GATEWAY_INTERNAL_URL = os.getenv(
    "PRIVATE_GATEWAY_INTERNAL_URL", "http://private-gateway-service.cloud-idp.svc:8080"
)

AIRFLOW_KEYCLOAK_ADMIN_ROLES = os.getenv("AIRFLOW_KEYCLOAK_ADMIN_ROLES", "airflow-role")

APP_URL_LOGOUT = os.getenv("APP_URL_LOGOUT")

SQLALCHEMY_DATABASE_URI = conf.get_mandatory_value("database", "SQL_ALCHEMY_CONN")

CSRF_ENABLED = True

basedir = os.path.abspath(os.path.dirname(__file__))

log = logging.getLogger(__name__)

AUTH_TYPE = AUTH_OAUTH

AUTH_USER_REGISTRATION = True

AUTH_USER_REGISTRATION_ROLE = "Public"

AUTH_ROLES_SYNC_AT_LOGIN = True

AUTH_ROLES_MAPPING = {
    "airflow_admin": ["Admin"],
    "airflow_op": ["Op"],
    "airflow_user": ["User"],
    "airflow_viewer": ["Viewer"],
    "airflow_public": ["Public"],
}

OAUTH_PROVIDERS = [
    {
        "name": "keycloak",
        "icon": "fa-key",
        "token_key": "access_token",
        "remote_app": {
            "client_id": CLIENT_ID,
            # "client_secret": "KEYCLOAK_CLIENT_SECRET",
            "api_base_url": f"{PRIVATE_GATEWAY_INTERNAL_URL}/auth/realms/{REALM_ID}/protocol/openid-connect",
            "access_token_url": f"{PRIVATE_GATEWAY_INTERNAL_URL}/auth/realms/{REALM_ID}/protocol/openid-connect/token",
            "authorize_url": f"{PRIVATE_GATEWAY_EXTERNAL_URL}/auth/realms/{REALM_ID}/protocol/openid-connect/auth",
            "logout_redirect_url": f"{PRIVATE_GATEWAY_EXTERNAL_URL}/auth/realms/{REALM_ID}/protocol/openid-connect/logout?redirect_uri={APP_URL_LOGOUT}",  # noqa E501
            "request_token_url": None,
        },
    }
]


class CustomAuthRemoteUserView(AuthOAuthView):
    @expose("/logout/")
    def logout(self):
        return super().logout()


class CustomSecurityManager(FabAirflowSecurityManagerOverride):
    authoauthview = CustomAuthRemoteUserView

    def oauth_user_info(self, provider, response):
        if provider == "keycloak":
            token = response["access_token"]

            me = jwt.decode(
                token, algorithms="RS256", options={"verify_signature": False}
            )

            user_id = me["sub"]

            headers = {
                "accept": "*/*",
                "Authorization": f"Bearer {token}",
            }

            response = requests.get(
                f"{PRIVATE_GATEWAY_INTERNAL_URL}/api/v1/user-management/scim/users/{user_id}",
                headers=headers,
            )
            user_info = response.json()

            log.debug(f"user info: {user_info}")
            log.debug("admin role is " + AIRFLOW_KEYCLOAK_ADMIN_ROLES)
            airflow_keycloak_admin_roles_list = AIRFLOW_KEYCLOAK_ADMIN_ROLES.split(",")

            for ADMIN_ROLE in airflow_keycloak_admin_roles_list:
                if (
                    ADMIN_ROLE in user_info["effectiveRealmRoles"]
                    or ADMIN_ROLE in user_info["realmRoles"]
                ):
                    groups = ["airflow_admin"]
                    log.debug(
                        "admin role "
                        + ADMIN_ROLE
                        + " is assigned to user so user will be ADMIN!"
                    )
                    break
            else:
                groups = ["airflow_viewer"]

            userinfo = {
                "username": user_info["username"],
                "email": f"{user_info['username']}@qubership.com",
                "first_name": user_info["username"],
                "last_name": user_info["username"],
                "role_keys": groups,
            }
            log.info("user info: {0}".format(userinfo))
            return userinfo
        else:
            return {}


SECURITY_MANAGER_CLASS = CustomSecurityManager
