"""
Configuración centralizada del microservicio usando Pydantic Settings.
Soporta inyección por variables de entorno y archivo .env.
"""

import os
import re
from typing import Optional
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _clean_url_str(val: str) -> str:
    """Limpia URLs que puedan contener sintaxis markdown accidental como [url](url)."""
    if not val:
        return val
    # Si viene en formato markdown [url](url), extraer el destino
    match = re.search(r"\[.*?\]\((https?://[^\s\)]+)\)", val)
    if match:
        return match.group(1).strip()
    return val.strip().strip("'\"")


class Settings(BaseSettings):
    """Parámetros de configuración de la aplicación."""

    model_config = SettingsConfigDict(
        env_file=(".env",),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Configuración de Odoo ERP
    ODOO_BASE_URL: str = Field(
        default="http://10.0.62.100:8069",
        description="URL base del servidor Odoo",
    )
    ODOO_ENDPOINT: str = Field(
        default="/quickbot_create_helpdesk_ticket/",
        description="Endpoint específico de Odoo para creación de tickets",
    )
    ODOO_TIMEOUT_SECONDS: float = Field(
        default=300.0,
        description="Timeout máximo en segundos para la llamada a Odoo (default: 300s / 5min)",
    )
    ODOO_MAX_RETRIES: int = Field(
        default=2,
        description="Cantidad máxima de reintentos ante fallos 5xx o de red",
    )
    ODOO_RETRY_BACKOFF_SECONDS: float = Field(
        default=3.0,
        description="Tiempo de espera en segundos entre reintentos",
    )
    SIMULATE_ODOO_DELAY_SECONDS: float = Field(
        default=0.0,
        ge=0.0,
        description="Latencia artificial en segundos antes de llamar a Odoo (para pruebas de estrés)",
    )

    # Cabeceras de autenticación parametrizables
    ODOO_HEADER_KEY_NAME: Optional[str] = Field(
        default="x-quickbot-token",
        description="Nombre de la cabecera primaria de autenticación",
    )
    ODOO_HEADER_KEY_VALUE: Optional[str] = Field(
        default="0a716b0b-058c-41b8-a111-1834745d0fc0",
        description="Valor de la cabecera primaria de autenticación",
    )
    ODOO_HEADER_TOKEN_NAME: Optional[str] = Field(
        default=None,
        description="Nombre de la cabecera secundaria de autenticación (opcional)",
    )
    ODOO_HEADER_TOKEN_VALUE: Optional[str] = Field(
        default=None,
        description="Valor de la cabecera secundaria de autenticación (opcional)",
    )

    # Gestión de workers y cola
    WORKER_CONCURRENCY: int = Field(
        default=5,
        ge=1,
        description="Número de workers asíncronos concurrentes que procesan la cola (default: 5)",
    )

    # Configuración de Redis Nativo
    REDIS_URL: str = Field(
        default="redis://127.0.0.1:6379/0",
        description="URI de conexión al servidor Redis nativo",
    )
    REDIS_MAX_CONNECTIONS: int = Field(
        default=20,
        ge=1,
        description="Tamaño máximo del pool de conexiones a Redis",
    )
    TASK_TTL_SECONDS: int = Field(
        default=86400,
        ge=60,
        description="Tiempo de retención en segundos (TTL) para cada registro de tarea en Redis (24 horas)",
    )
    IDEMPOTENCY_WINDOW_SECONDS: int = Field(
        default=60,
        ge=1,
        description="Ventana de tiempo en segundos para deduplicación e idempotencia",
    )

    # Notificador Artificialic (WhatsApp)
    MOCK_ARTIFICIALIC: bool = Field(
        default=True,
        description="Si es True, simula el envío del mensaje en logs sin hacer petición real",
    )
    ARTIFICIALIC_API_URL: str = Field(
        default="https://api.artificialic.com/v1/messages",
        description="URL de la API de Artificialic para notificaciones outbound",
    )
    ARTIFICIALIC_API_TOKEN: str = Field(
        default="mock_token",
        description="Token de autenticación de Artificialic",
    )
    ARTIFICIALIC_TIMEOUT_SECONDS: float = Field(
        default=15.0,
        description="Timeout para llamadas hacia Artificialic",
    )
    NOTIFIER_MAX_RETRIES: int = Field(
        default=3,
        ge=1,
        description="Máximo de reintentos para notificaciones de Artificialic",
    )
    NOTIFIER_RETRY_BACKOFF_SECONDS: float = Field(
        default=2.0,
        ge=0.1,
        description="Backoff base en segundos para reintentos de notificación",
    )
    SEND_FALLBACK_ON_ODOO_FAILURE: bool = Field(
        default=True,
        description="Enviar mensaje de contingencia al cliente si Odoo falla definitivamente",
    )

    # Servidor y Entorno
    APP_HOST: str = Field(default="0.0.0.0", description="Host de escucha")
    APP_PORT: int = Field(default=8000, description="Puerto de escucha")
    APP_ENV: str = Field(default="development", description="Entorno de ejecución")
    LOG_LEVEL: str = Field(default="INFO", description="Nivel de logs (DEBUG, INFO, etc.)")

    @field_validator("ODOO_BASE_URL", "ARTIFICIALIC_API_URL", mode="before")
    @classmethod
    def clean_urls(cls, value: str) -> str:
        if isinstance(value, str):
            return _clean_url_str(value)
        return value

    @property
    def odoo_full_url(self) -> str:
        """Construye la URL absoluta normalizada hacia el endpoint de Odoo."""
        base = self.ODOO_BASE_URL.rstrip("/")
        endpoint = self.ODOO_ENDPOINT.strip()
        if not endpoint.startswith("/"):
            endpoint = "/" + endpoint
        return f"{base}{endpoint}"


# Instancia singleton accesible globalmente
settings = Settings()
