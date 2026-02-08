import logging
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str
    LOG_LEVEL: str = "INFO"

    model_config = {
        "env_file": ".env",
        "extra": "ignore"
    }


settings = Settings()


def configure_logging():
    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
