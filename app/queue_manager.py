"""
Gestor de la cola distribuida respaldada en Redis (ticket:queue) y pool de workers en segundo plano.
Implementa consumo atómico bloqueante (BLPOP), seguimiento de tareas en vuelo (ticket:processing),
auto-recuperación tras reinicio/crash, concurrencia enterprise y notificación de contingencia ante fallos.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Union

from app.clients.notifier_client import notifier_client
from app.clients.odoo_client import odoo_client
from app.config import settings
from app.redis_client import get_redis
from app.schemas import TicketStatus
from app.state_store import state_store

logger = logging.getLogger(__name__)

REDIS_QUEUE_KEY = "ticket:queue"
REDIS_PROCESSING_KEY = "ticket:processing"


class QueueManager:
    """
    Administra la cola persistente en Redis (ticket:queue), el set de tareas en vuelo (ticket:processing)
    y el pool de workers asíncronos desacoplados.
    """

    def __init__(self):
        self._workers: List[asyncio.Task] = []
        self._is_running: bool = False

    async def get_queue_size(self) -> int:
        """Retorna la cantidad actual de tareas en espera en la lista de Redis."""
        try:
            redis = get_redis()
            return await redis.llen(REDIS_QUEUE_KEY)
        except Exception as err:
            logger.warning(f"⚠️ [QUEUE] Error al obtener tamaño de cola en Redis: {err}")
            return 0

    async def enqueue(self, task_id: str) -> None:
        """
        Inserta de forma atómica y persistente el task_id al final de la lista Redis (RPUSH).
        Operación O(1) de alta velocidad.
        """
        redis = get_redis()
        await redis.rpush(REDIS_QUEUE_KEY, task_id)

    async def recover_interrupted_tasks(self) -> int:
        """
        Recupera tareas en vuelo que quedaron huérfanas en 'ticket:processing'
        debido a un crash o reinicio abrupto del servicio.
        Re-encola cada tarea al inicio de ticket:queue (LPUSH) y restaura su estado a 'queued'.
        Si una tarea ya figuraba como 'completed', únicamente se remueve de ticket:processing.
        """
        try:
            redis = get_redis()
            interrupted_tasks = await redis.smembers(REDIS_PROCESSING_KEY)
            if not interrupted_tasks:
                return 0

            count = len(interrupted_tasks)
            logger.warning(
                f"🔄 [RECOVERY] Detectadas {count} tareas huérfanas en {REDIS_PROCESSING_KEY}. "
                f"Iniciando recuperación prioritaria hacia {REDIS_QUEUE_KEY}..."
            )

            recovered = 0
            for task_id in interrupted_tasks:
                # Defensa en profundidad: si la tarea ya estaba completada en Odoo,
                # solo la removemos de ticket:processing sin duplicar la creación del ticket.
                task_data = await state_store.get_task(task_id)
                if task_data and task_data.status == TicketStatus.COMPLETED:
                    logger.info(
                        f"ℹ️ [RECOVERY] Tarea {task_id} ya estaba completada (Ticket #{task_data.ticket_id}). "
                        f"Removiendo de {REDIS_PROCESSING_KEY} sin re-encolar."
                    )
                    await redis.srem(REDIS_PROCESSING_KEY, task_id)
                    continue

                async with redis.pipeline(transaction=True) as pipe:
                    pipe.lpush(REDIS_QUEUE_KEY, task_id)
                    pipe.srem(REDIS_PROCESSING_KEY, task_id)
                    await pipe.execute()
                await state_store.set_queued(task_id)
                recovered += 1

            logger.info(f"✅ [RECOVERY] Se recuperaron {recovered} tareas exitosamente y fueron devueltas a la cola.")
            return recovered
        except Exception as rec_err:
            logger.error(f"❌ [RECOVERY] Error durante la auto-recuperación de tareas: {rec_err}")
            return 0

    async def _worker_loop(self, worker_id: int) -> None:
        """
        Bucle continuo de consumo respaldado en BLPOP sobre la lista ticket:queue
        con seguimiento atómico en ticket:processing.
        """
        logger.info(f"⚙️ [WORKER] Worker-{worker_id} iniciado y a la escucha en {REDIS_QUEUE_KEY}.")

        while self._is_running:
            task_id: Optional[str] = None
            was_cancelled: bool = False
            is_completed: bool = False
            raw_data: Optional[Dict[str, str]] = None
            try:
                redis = get_redis()
                # Extracción atómica con timeout de 2s para permitir chequeo de señal de apagado
                result = await redis.blpop([REDIS_QUEUE_KEY], timeout=2)
                if result is None:
                    continue  # Timeout transcurrido sin tareas, continuar escuchando

                _, task_id = result

                # Registrar inmediatamente en el conjunto de tareas en procesamiento
                await redis.sadd(REDIS_PROCESSING_KEY, task_id)

            except asyncio.CancelledError:
                if task_id:
                    try:
                        redis = get_redis()
                        await redis.sadd(REDIS_PROCESSING_KEY, task_id)
                    except Exception:
                        pass
                logger.info(f"⚙️ [WORKER] Worker-{worker_id} recibió señal de cancelación.")
                break
            except Exception as conn_err:
                if self._is_running:
                    logger.warning(f"⚠️ [WORKER] Worker-{worker_id} error de conexión con Redis: {conn_err}. Reintentando en 1s...")
                    await asyncio.sleep(1)
                continue

            state_store.increment_active_workers()

            try:
                # 1. Recuperar los datos del registro desde el Hash de Redis
                raw_data = await state_store.get_raw_task_data(task_id)
                if not raw_data:
                    logger.warning(f"⚠️ [WORKER] Worker-{worker_id} tarea {task_id} no encontrada en Redis (posible TTL expirado).")
                    continue

                remaining_queue = await self.get_queue_size()
                logger.info(
                    f"⚙️ [WORKER] Worker-{worker_id} tomando tarea {task_id} | "
                    f"Orden: {raw_data.get('order_name')} | Cliente: {raw_data.get('partner_id')} | "
                    f"Pendientes en cola: {remaining_queue}"
                )

                # 2. Marcar estado como 'processing_odoo' en Redis
                await state_store.set_processing(task_id)

                # 3. Construir payload filtrado y llamar a Odoo ERP
                payload = {
                    "order_name": raw_data.get("order_name", ""),
                    "partner_id": int(raw_data.get("partner_id", 0)),
                    "team_id": int(raw_data.get("team_id", 1)),
                    "ticket_comment": raw_data.get("ticket_comment", ""),
                }

                ticket_id, duration_seconds, _ = await odoo_client.create_ticket(
                    payload=payload,
                    task_id=task_id,
                )

                # 4. Registrar éxito y métricas en Redis
                await state_store.set_completed(
                    task_id=task_id,
                    ticket_id=ticket_id,
                    duration_seconds=duration_seconds,
                )
                is_completed = True
                logger.info(
                    f"✅ [COMPLETED] Tarea {task_id} finalizada exitosamente. "
                    f"Ticket ID: #{ticket_id} (Latencia Odoo: {duration_seconds:.2f}s)"
                )

                # 5. Notificar al cliente vía WhatsApp (Artificialic)
                await notifier_client.send_ticket_notification(
                    telefono=raw_data.get("telefono", ""),
                    ticket_id=ticket_id,
                    duration_seconds=duration_seconds,
                    task_id=task_id,
                )

            except asyncio.CancelledError:
                if not is_completed:
                    was_cancelled = True
                    logger.warning(
                        f"⚙️ [WORKER] Worker-{worker_id} cancelado durante el procesamiento de {task_id}. "
                        f"Preservando tarea intacta en {REDIS_PROCESSING_KEY} para auto-recuperación al reiniciar."
                    )
                else:
                    logger.warning(
                        f"⚙️ [WORKER] Worker-{worker_id} cancelado tras completar Odoo para {task_id}. "
                        f"El ticket ya fue creado; no se re-encolará."
                    )
                raise

            except Exception as task_err:
                error_detail = str(task_err)
                if task_id:
                    await state_store.set_failed(task_id=task_id, error_detail=error_detail)
                logger.error(f"❌ [FAILED] Tarea {task_id} falló: {error_detail}")

                # Disparo de notificación de contingencia para que el cliente no quede en el limbo
                if settings.SEND_FALLBACK_ON_ODOO_FAILURE and raw_data:
                    try:
                        await notifier_client.send_failure_fallback_notification(
                            telefono=raw_data.get("telefono", ""),
                            order_name=raw_data.get("order_name", ""),
                            task_id=task_id or "unknown",
                        )
                    except Exception as fallback_err:
                        logger.error(f"⚠️ [NOTIFICADOR] Fallo al enviar mensaje de contingencia: {fallback_err}")

            finally:
                # Quitar tarea del Set de procesamiento activo SOLO si no fue interrumpida por apagado (cancellation)
                if task_id and not was_cancelled:
                    try:
                        redis = get_redis()
                        await redis.srem(REDIS_PROCESSING_KEY, task_id)
                    except Exception as cleanup_err:
                        logger.warning(f"⚠️ [WORKER] Error al remover {task_id} de {REDIS_PROCESSING_KEY}: {cleanup_err}")

                state_store.decrement_active_workers()

        logger.info(f"⚙️ [WORKER] Worker-{worker_id} detenido limpiamente.")

    async def start(self) -> None:
        """Arranca el grupo de workers según la concurrencia configurada tras recuperar tareas huérfanas."""
        if self._is_running:
            return

        self._is_running = True

        # Recuperación prioritaria de tareas huérfanas tras caída/reinicio
        await self.recover_interrupted_tasks()

        concurrency = settings.WORKER_CONCURRENCY
        logger.info(f"⚙️ [WORKER] Iniciando pool de {concurrency} workers asíncronos sobre Redis...")

        self._workers = [
            asyncio.create_task(self._worker_loop(worker_id=i + 1), name=f"redis-worker-{i+1}")
            for i in range(concurrency)
        ]

    async def stop(self) -> None:
        """Detiene de forma limpia todos los workers cancelando las tareas activas."""
        if not self._is_running:
            return

        logger.info("⚙️ [WORKER] Señal de apagado recibida. Deteniendo workers...")
        self._is_running = False

        for worker in self._workers:
            worker.cancel()

        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
            self._workers.clear()

        logger.info("⚙️ [WORKER] Todos los workers han sido detenidos correctamente.")


# Instancia singleton del gestor de cola
queue_manager = QueueManager()
