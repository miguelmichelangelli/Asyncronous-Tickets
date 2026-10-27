"""
Almacén de estado persistente basado en Redis Hashes (ticket:task:{task_id}) con TTL automático
y contadores atómicos para métricas operativas.
"""

from datetime import datetime, timezone
import logging
from typing import Any, Dict, Optional, Union

from app.config import settings
from app.redis_client import get_redis
from app.schemas import (
    QueueMetricsResponse,
    TicketEnqueueRequest,
    TicketStatus,
    TicketStatusResponse,
)

logger = logging.getLogger(__name__)


class StateStore:
    """
    Gestiona el ciclo de vida de las tareas y métricas respaldadas en Redis nativo.
    Elimina la volatilidad del proceso y garantiza auto-expiración (TTL) sin fugas de memoria.
    """

    def __init__(self):
        self._active_workers: int = 0

    @staticmethod
    def _task_key(task_id: str) -> str:
        return f"ticket:task:{task_id}"

    async def register_task(
        self,
        task_id: str,
        request_data: TicketEnqueueRequest,
        idemp_key: str = "",
    ) -> None:
        """
        Registra una nueva tarea en un Hash de Redis con estado inicial 'queued',
        fija el TTL de 24 horas, vincula idemp_key y marca notification_sent='false'.
        """
        redis = get_redis()
        key = self._task_key(task_id)
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        mapping = {
            "task_id": task_id,
            "status": TicketStatus.QUEUED.value,
            "order_name": request_data.order_name,
            "partner_id": str(request_data.partner_id),
            "team_id": str(request_data.team_id),
            "ticket_comment": request_data.ticket_comment,
            "ticket_title": request_data.ticket_title or "",
            "telefono": request_data.telefono,
            "ticket_id": "",
            "duration_seconds": "",
            "error_detail": "",
            "idemp_key": idemp_key,
            "notification_sent": "false",
            "created_at": now_iso,
            "updated_at": now_iso,
        }

        # Ejecución atómica en pipeline: HSET + EXPIRE
        async with redis.pipeline(transaction=True) as pipe:
            pipe.hset(key, mapping=mapping)
            pipe.expire(key, settings.TASK_TTL_SECONDS)
            await pipe.execute()

    async def get_task(self, task_id: str) -> Optional[TicketStatusResponse]:
        """
        Recupera y deserializa una tarea desde Redis Hash retornando TicketStatusResponse.
        """
        redis = get_redis()
        key = self._task_key(task_id)
        data: Dict[str, str] = await redis.hgetall(key)

        if not data:
            return None

        # Deserializar campos opcionales
        ticket_id_val: Optional[Union[int, str]] = None
        raw_ticket_id = data.get("ticket_id", "").strip()
        if raw_ticket_id:
            ticket_id_val = int(raw_ticket_id) if raw_ticket_id.isdigit() else raw_ticket_id

        duration_val: Optional[float] = None
        raw_duration = data.get("duration_seconds", "").strip()
        if raw_duration:
            try:
                duration_val = float(raw_duration)
            except ValueError:
                pass

        error_detail_val = data.get("error_detail", "").strip() or None
        notification_sent_val = data.get("notification_sent", "").lower() in ("true", "1", "yes")

        return TicketStatusResponse(
            task_id=data.get("task_id", task_id),
            status=TicketStatus(data.get("status", TicketStatus.QUEUED.value)),
            ticket_id=ticket_id_val,
            duration_seconds=duration_val,
            created_at=data.get("created_at", ""),
            error_detail=error_detail_val,
            notification_sent=notification_sent_val,
        )

    async def get_raw_task_data(self, task_id: str) -> Optional[Dict[str, str]]:
        """
        Retorna el diccionario crudo del Hash para que el worker consuma los parámetros necesarios.
        """
        redis = get_redis()
        key = self._task_key(task_id)
        data = await redis.hgetall(key)
        return data if data else None

    async def set_processing(self, task_id: str) -> None:
        """Actualiza el estado de la tarea a 'processing_odoo' en Redis."""
        redis = get_redis()
        key = self._task_key(task_id)
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        await redis.hset(
            key,
            mapping={
                "status": TicketStatus.PROCESSING_ODOO.value,
                "updated_at": now_iso,
            },
        )

    async def set_queued(self, task_id: str) -> None:
        """Restablece el estado de una tarea a 'queued' tras recuperación ante caída/reinicio."""
        redis = get_redis()
        key = self._task_key(task_id)
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        await redis.hset(
            key,
            mapping={
                "status": TicketStatus.QUEUED.value,
                "updated_at": now_iso,
            },
        )

    async def set_notification_status(self, task_id: str, sent: bool) -> None:
        """Actualiza en Redis si la notificación al cliente fue despachada con éxito."""
        redis = get_redis()
        key = self._task_key(task_id)
        await redis.hset(key, "notification_sent", "true" if sent else "false")

    async def set_completed(
        self,
        task_id: str,
        ticket_id: Union[int, str],
        duration_seconds: float,
    ) -> None:
        """
        Marca la tarea como 'completed' en Redis e incrementa atómicamente el contador de métricas.
        """
        redis = get_redis()
        key = self._task_key(task_id)
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        async with redis.pipeline(transaction=True) as pipe:
            pipe.hset(
                key,
                mapping={
                    "status": TicketStatus.COMPLETED.value,
                    "ticket_id": str(ticket_id),
                    "duration_seconds": str(round(duration_seconds, 2)),
                    "error_detail": "",
                    "updated_at": now_iso,
                },
            )
            pipe.incr("ticket:metrics:completed")
            await pipe.execute()

    async def set_failed(
        self,
        task_id: str,
        error_detail: str,
        duration_seconds: Optional[float] = None,
    ) -> None:
        """
        Marca la tarea como 'failed' en Redis, registra el detalle del error,
        libera el candado de idempotencia si existe e incrementa métricas de fallo.
        """
        redis = get_redis()
        key = self._task_key(task_id)
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        # Recuperar idemp_key si está vinculado para liberar el candado
        idemp_key = await redis.hget(key, "idemp_key")

        mapping: Dict[str, str] = {
            "status": TicketStatus.FAILED.value,
            "error_detail": error_detail,
            "updated_at": now_iso,
        }
        if duration_seconds is not None:
            mapping["duration_seconds"] = str(round(duration_seconds, 2))

        async with redis.pipeline(transaction=True) as pipe:
            pipe.hset(key, mapping=mapping)
            pipe.incr("ticket:metrics:failed")
            if idemp_key:
                pipe.delete(idemp_key)
            await pipe.execute()

    def increment_active_workers(self) -> None:
        """Incrementa el contador en memoria de workers procesando activamente."""
        self._active_workers += 1

    def decrement_active_workers(self) -> None:
        """Decrementa el contador en memoria de workers procesando activamente."""
        if self._active_workers > 0:
            self._active_workers -= 1

    async def get_metrics(self, queue_size: int) -> QueueMetricsResponse:
        """
        Consulta los contadores persistentes en Redis y retorna una instantánea de métricas.
        """
        redis = get_redis()
        metrics = await redis.mget("ticket:metrics:completed", "ticket:metrics:failed")

        completed_total = int(metrics[0]) if metrics[0] is not None else 0
        failed_total = int(metrics[1]) if metrics[1] is not None else 0

        return QueueMetricsResponse(
            queued_tasks=queue_size,
            active_workers=self._active_workers,
            completed_total=completed_total,
            failed_total=failed_total,
        )


# Instancia singleton para toda la aplicación
state_store = StateStore()
