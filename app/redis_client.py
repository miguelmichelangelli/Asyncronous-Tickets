"""
Cliente Redis Asíncrono Singleton basado en redis.asyncio.
Gestiona el pool de conexiones centralizado, reconexión automática y ciclo de vida de FastAPI.
"""

import logging
from typing import Optional
from redis.asyncio import ConnectionPool, Redis

from app.config import settings

logger = logging.getLogger(__name__)

_pool: Optional[ConnectionPool] = None
_redis: Optional[Redis] = None


async def init_redis() -> Redis:
    """
    Inicializa el ConnectionPool y el cliente Redis singleton.
    Aplica reconexión por timeout, health check periódico y decodificación automática UTF-8.
    """
    global _pool, _redis
    if _redis is None:
        logger.info(f"🔌 [REDIS] Inicializando ConnectionPool hacia {settings.REDIS_URL} (max_connections={settings.REDIS_MAX_CONNECTIONS})...")
        _pool = ConnectionPool.from_url(
            url=settings.REDIS_URL,
            max_connections=settings.REDIS_MAX_CONNECTIONS,
            decode_responses=True,
            health_check_interval=30,
            retry_on_timeout=True,
            protocol=2,  # <--- AGREGAR ESTA LÍNEA (Fuerza protocolo RESP2)
        )
        _redis = Redis(connection_pool=_pool)
    return _redis


def get_redis() -> Redis:
    """
    Retorna la instancia activa del cliente Redis.
    Si no ha sido inicializada, la instancia se crea de forma inmediata.
    """
    global _pool, _redis
    if _redis is None:
        _pool = ConnectionPool.from_url(
            url=settings.REDIS_URL,
            max_connections=settings.REDIS_MAX_CONNECTIONS,
            decode_responses=True,
            health_check_interval=30,
            retry_on_timeout=True,
            protocol=2,  # <--- AGREGAR ESTA LÍNEA (Fuerza protocolo RESP2)
        )
        _redis = Redis(connection_pool=_pool)
    return _redis


def set_redis(redis_instance: Optional[Redis]) -> None:
    """Permite inyectar un cliente mock o fakeredis para suites de prueba."""
    global _redis
    _redis = redis_instance


async def close_redis() -> None:
    """
    Cierra limpiamente el cliente y libera todas las conexiones activas del pool.
    """
    global _pool, _redis
    if _redis is not None:
        logger.info("🛑 [REDIS] Cerrando conexiones con Redis...")
        await _redis.aclose()
        _redis = None
    if _pool is not None:
        await _pool.aclose()
        _pool = None
    logger.info("🛑 [REDIS] Conexiones liberadas exitosamente.")

