"""Auth-related settings mixin. Apps should inherit from AuthSettings in their own Settings class."""
from pydantic_settings import BaseSettings


class AuthSettings(BaseSettings):
    jwt_secret: str = "change-me-to-a-random-secret-key"
    jwt_expiry_hours: int = 24
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_tls: bool = True
    smtp_user: str = ""
    smtp_password: str = ""
    smtp_from: str = "noreply@localhost"

    model_config = {"extra": "ignore"}
