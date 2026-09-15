"""Environment-driven configuration for the emulator.

Settings are loaded once and cached; call get_settings() anywhere a
component needs config rather than importing a module-level instance,
so tests can override via dependency injection if needed later.
"""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="EMULATOR_", env_file=".env", extra="ignore")

    port: int = 4566
    data_dir: str = "./data"
    log_level: str = "INFO"
    test_access_key: str = "test"
    test_secret_key: str = "test"
    enable_signature_check: bool = False


@lru_cache
def get_settings() -> Settings:
    return Settings()