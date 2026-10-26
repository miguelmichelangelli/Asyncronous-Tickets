"""
Punto de entrada principal de FastAPI para el microservicio quickbot_ticket_service.
Define los endpoints de la API, el ciclo de vida (lifespan) con Redis Nativo e Idempotencia estricta.
"""

import asyncio
from contextlib import asynccontextmanager
import hashlib
import json
import logging
import sys
import uuid
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import redis.exceptions as redis_exc
from starlette.requests import Request

# Reconfigurar stdout y stderr a UTF-8 para soporte de emojis en Windows/consolas legacy
if sys.platform.startswith("win"):
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from app.clients.notifier_client import notifier_client
from app.clients.odoo_client import odoo_client
from app.config import settings
from app.queue_manager import queue_manager
from app.redis_client import close_redis, get_redis, init_redis
from app.schemas import (
    QueueMetricsResponse,
    TicketEnqueueRequest,
    TicketEnqueueResponse,
    TicketStatus,
    TicketStatusResponse,
)
from app.state_store import state_store

# Configuración de Logging Estructurado
_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
    handlers=[_handler],
)
logger = logging.getLogger("quickbot_service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Gestiona el ciclo de vida de la aplicación con Redis:
    - Conecta e inicializa el ConnectionPool a Redis y valida conectividad con PING.
    - Arranca el pool de workers en segundo plano al iniciar.
    - Detiene ordenadamente los workers y cierra clientes HTTP y Redis al apagar.
    """
    logger.info("================================================================")
    logger.info("🚀 [STARTUP] Iniciando Quickbot Ticket Buffer Service con Redis...")
    logger.info(f"🚀 [STARTUP] Redis Target: {settings.REDIS_URL}")
    logger.info(f"🚀 [STARTUP] Odoo Target: {settings.odoo_full_url}")
    logger.info(f"🚀 [STARTUP] Concurrencia de Workers: {settings.WORKER_CONCURRENCY}")
    logger.info(f"🚀 [STARTUP] Modo Mock Notificador: {settings.MOCK_ARTIFICIALIC}")
    logger.info(f"🚀 [STARTUP] Task TTL: {settings.TASK_TTL_SECONDS}s | Idempotencia: {settings.IDEMPOTENCY_WINDOW_SECONDS}s")
    logger.info("================================================================")

    # 1. Inicializar cliente y conexión Redis con verificación PING
    try:
        redis = await init_redis()
        await redis.ping()
        logger.info("✅ [STARTUP] Conexión con Redis establecida exitosamente (PING OK).")
    except Exception as redis_err:
        logger.error(f"❌ [STARTUP] Error crítico al conectar con Redis ({settings.REDIS_URL}): {redis_err}")
        raise redis_err

    # 2. Iniciar pool de workers consumidores
    await queue_manager.start()

    yield

    # 3. Apagado limpio y ordenado
    logger.info("🛑 [SHUTDOWN] Deteniendo Quickbot Ticket Service...")
    await queue_manager.stop()
    await odoo_client.close()
    await notifier_client.close()
    await close_redis()
    logger.info("🛑 [SHUTDOWN] Apagado completado exitosamente.")


# Inicialización de la Aplicación FastAPI
app = FastAPI(
    title="Quickbot Ticket Buffer Service (Redis Native)",
    description=(
        "Microservicio asíncrono para desacoplar la ingesta de tickets de soporte en WhatsApp "
        "de la alta latencia del ERP Odoo mediante patrón Productor-Consumidor respaldado en Redis."
    ),
    version="2.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# Middleware de CORS permisivo para integraciones web y Swagger
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def sanitize_raw_json_control_chars_middleware(request: Request, call_next):
    """
    Middleware defensivo para sanear payloads JSON con caracteres de control literales (\n, \r, \t)
    dentro de strings (habitual en WhatsApp vía Artificialic cuando el usuario escribe con Enter).
    Interviene en peticiones POST hacia /api/tickets/enqueue (con o sin trailing slash).
    """
    if request.method == "POST" and request.url.path.rstrip("/") == "/api/tickets/enqueue":
        try:
            raw_body = await request.body()
            if raw_body:
                try:
                    # Validar si cumple JSON estricto estándar
                    json.loads(raw_body)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    # Si falla por caracteres de control literales, deserializar con strict=False
                    try:
                        decoded_str = raw_body.decode("utf-8", errors="replace")
                        parsed = json.loads(decoded_str, strict=False)
                        # Re-empaquetar con secuencias de escape válidas (ej. \n escapado)
                        clean_body = json.dumps(parsed).encode("utf-8")
                        request._body = clean_body
                        if hasattr(request, "_json"):
                            delattr(request, "_json")
                    except Exception as parse_err:
                        logger.warning(f"⚠️ [MIDDLEWARE] No fue posible sanear JSON con strict=False: {parse_err}")
        except Exception as req_err:
            logger.warning(f"⚠️ [MIDDLEWARE] Error procesando cuerpo de la petición: {req_err}")

    response = await call_next(request)
    return response


# Excepciones de Redis/Red candidatas a reintento rápido en ingesta
REDIS_RETRYABLE_EXCEPTIONS = (
    redis_exc.RedisError,
    ConnectionError,
    TimeoutError,
    OSError,
)


@app.get("/", tags=["Health"])
async def root():
    """Endpoint raíz con estado básico y enlace a documentación."""
    return {
        "service": "quickbot_ticket_service",
        "version": "2.0.0 (Redis Native)",
        "status": "online",
        "docs": "/docs",
        "workers": settings.WORKER_CONCURRENCY,
    }


@app.get("/health", tags=["Health"])
async def health_check():
    """Health check con validación de Redis y tamaño de cola en tiempo real."""
    redis_healthy = False
    try:
        redis = get_redis()
        redis_healthy = await redis.ping()
    except Exception:
        pass

    queue_size = await queue_manager.get_queue_size()
    return {
        "status": "healthy" if redis_healthy else "degraded",
        "redis_connected": redis_healthy,
        "queue_size": queue_size,
        "odoo_target": settings.odoo_full_url,
    }


@app.post(
    "/api/tickets/enqueue",
    response_model=TicketEnqueueResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Encolar solicitud de ticket (Idempotente y persistente en Redis)",
    description=(
        "Valida la solicitud, aplica ventana de idempotencia (60s) contra duplicados en Redis, "
        "registra el Hash de la tarea con TTL de 24h, inserta en ticket:queue y retorna HTTP 202 en < 50ms."
    ),
    tags=["Tickets"],
)
@app.post(
    "/api/tickets/enqueue/",
    response_model=TicketEnqueueResponse,
    status_code=status.HTTP_202_ACCEPTED,
    include_in_schema=False,
)
async def enqueue_ticket(request: TicketEnqueueRequest):
    """
    Endpoint productor con tolerancia a micro-cortes de Redis y liberación de candado ante fallos:
    - Genera clave de idempotencia hash SHA-256.
    - Adquiere bloqueo atómico SET NX en Redis. Si es duplicado pero la tarea previa falló, permite reintento.
    - Si es nuevo, registra el Hash con TTL de 24h vinculando idemp_key, empuja a ticket:queue y responde 202.
    - Reintenta hasta 2 veces ante micro-cortes de Redis antes de responder HTTP 503.
    """
    raw_idemp = f"{request.partner_id}:{request.order_name}:{request.team_id}:{request.ticket_comment.strip()}"
    idemp_hash = hashlib.sha256(raw_idemp.encode()).hexdigest()[:16]
    idemp_key = f"ticket:idemp:{idemp_hash}"
    candidate_task_id = uuid.uuid4().hex[:12]

    max_redis_retries = 2
    for attempt in range(max_redis_retries + 1):
        try:
            redis = get_redis()

            # 1. Intentar registrar candado de idempotencia con SET NX y TTL configurable
            acquired = await redis.set(
                idemp_key,
                candidate_task_id,
                ex=settings.IDEMPOTENCY_WINDOW_SECONDS,
                nx=True,
            )

            # 2. Caso: Solicitud duplicada detectada dentro de la ventana de tiempo
            if not acquired:
                existing_task_id = await redis.get(idemp_key)
                is_previous_failed = False
                if existing_task_id:
                    existing_task = await state_store.get_task(existing_task_id)
                    if existing_task and existing_task.status == TicketStatus.FAILED:
                        is_previous_failed = True

                if is_previous_failed:
                    logger.info(
                        f"🔄 [IDEMPOTENCIA] Tarea previa {existing_task_id} para orden '{request.order_name}' "
                        f"terminó en FAILED. Sobrescribiendo candado y permitiendo reintento inmediato."
                    )
                    await redis.set(
                        idemp_key,
                        candidate_task_id,
                        ex=settings.IDEMPOTENCY_WINDOW_SECONDS,
                    )
                    task_id = candidate_task_id
                else:
                    task_id_to_return = existing_task_id if existing_task_id else candidate_task_id
                    logger.warning(
                        f"⚠️ [IDEMPOTENCIA] Solicitud duplicada detectada para orden '{request.order_name}', "
                        f"cliente {request.partner_id}. Reutilizando task_id={task_id_to_return}"
                    )
                    return TicketEnqueueResponse(
                        status=TicketStatus.QUEUED,
                        task_id=task_id_to_return,
                        message="Solicitud duplicada detectada. Procesando requerimiento previo.",
                    )
            else:
                task_id = candidate_task_id

            logger.info(
                f"📥 [ENQUEUE] Tarea recibida: task_id={task_id} | Tel: {request.telefono} | "
                f"Orden: {request.order_name} | Partner: {request.partner_id}"
            )

            # 3. Registrar en Redis Hash con TTL automático (24h) e idemp_key vinculado
            await state_store.register_task(
                task_id=task_id,
                request_data=request,
                idemp_key=idemp_key,
            )

            # 4. Encolar de forma atómica en la lista persistente ticket:queue
            await queue_manager.enqueue(task_id=task_id)

            # 5. Retornar inmediatamente HTTP 202 Accepted
            return TicketEnqueueResponse(
                status=TicketStatus.QUEUED,
                task_id=task_id,
                message="Solicitud de ticket recibida y encolada exitosamente.",
            )

        except REDIS_RETRYABLE_EXCEPTIONS as redis_err:
            if attempt < max_redis_retries:
                logger.warning(
                    f"⚠️ [REDIS] Micro-corte detectado en enqueue_ticket ({redis_err}). "
                    f"Reintentando en 150ms (intento {attempt + 1}/{max_redis_retries})..."
                )
                await asyncio.sleep(0.15)
                continue
            logger.error(f"❌ [REDIS] Error persistente de conexión a Redis en enqueue tras reintentos: {redis_err}")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Servicio de colas temporalmente ocupado, reintente",
            )


@app.get(
    "/api/tickets/status/{task_id}",
    response_model=TicketStatusResponse,
    summary="Consultar estado de una tarea por su task_id",
    description="Retorna el estado en tiempo real desde Redis Hash (queued, processing_odoo, completed, failed).",
    tags=["Tickets"],
)
async def get_ticket_status(task_id: str):
    """
    Consulta el estado actual de la tarea desde su Hash en Redis.
    Retorna 404 si la tarea no existe o ha expirado su TTL.
    """
    task = await state_store.get_task(task_id)
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Tarea con ID '{task_id}' no encontrada en el registro (posible TTL expirado).",
        )
    return task


@app.get(
    "/api/tickets/queue-metrics",
    response_model=QueueMetricsResponse,
    summary="Consultar métricas operativas de la cola y workers",
    description="Proporciona visibilidad del tamaño de ticket:queue, workers activos, completados y fallidos.",
    tags=["Metrics"],
)
async def get_queue_metrics():
    """
    Retorna una instantánea del estado de procesamiento desde contadores de Redis.
    """
    queue_size = await queue_manager.get_queue_size()
    return await state_store.get_metrics(queue_size=queue_size)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.APP_HOST,
        port=settings.APP_PORT,
        reload=settings.APP_ENV == "development",
        log_level=settings.LOG_LEVEL.lower(),
    )
