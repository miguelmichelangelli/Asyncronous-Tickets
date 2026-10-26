"""
Cliente para envío de notificaciones outbound al cliente (Artificialic / WhatsApp Meta).
Incluye reintentos con backoff exponencial (2s, 4s, 8s) ante 5xx/429, modo simulado (MOCK_ARTIFICIALIC),
mensaje de contingencia ante caídas de Odoo y persistencia del estado en Redis (notification_sent).
"""

import asyncio
import logging
from typing import Optional, Union
import httpx

from app.config import settings
from app.state_store import state_store

logger = logging.getLogger(__name__)


class NotifierClient:
    """Gestiona el envío resiliente de mensajes outbound y mensajes de contingencia."""

    def __init__(self):
        self._client: Optional[httpx.AsyncClient] = None

    async def get_client(self) -> httpx.AsyncClient:
        """Inicializa o retorna la instancia httpx.AsyncClient para llamadas a Artificialic."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(settings.ARTIFICIALIC_TIMEOUT_SECONDS),
                limits=httpx.Limits(max_keepalive_connections=10, max_connections=20),
            )
        return self._client

    async def close(self) -> None:
        """Cierra el cliente HTTP liberando recursos."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def _dispatch_with_retry(
        self,
        telefono: str,
        message_body: str,
        task_id: str,
        notification_type: str = "TICKET_OK",
    ) -> bool:
        """
        Envía la petición HTTP a Artificialic aplicando backoff exponencial ante 5xx, 429 o red.
        Persiste el resultado 'notification_sent' en Redis.
        """
        client = await self.get_client()
        headers = {
            "Authorization": f"Bearer {settings.ARTIFICIALIC_API_TOKEN}",
            "Content-Type": "application/json",
        }
        # Normalizar teléfono con prefijo internacional
        clean_phone = telefono.lstrip("+")
        phone_formatted = f"+{clean_phone}" if clean_phone else telefono

        payload = {
            # "phoneNumber": phone_formatted,
            "phoneNumber": '584123953003',
            "channelType": "WHATSAPP",
            "content": message_body,
            "metadata": {
                "task_id": task_id,
                "type": notification_type,
            },
        }

        max_retries = max(1, settings.NOTIFIER_MAX_RETRIES)
        base_backoff = settings.NOTIFIER_RETRY_BACKOFF_SECONDS

        for attempt in range(1, max_retries + 1):
            try:
                logger.info(
                    f"📲 [NOTIFICADOR] [Task {task_id}] ({notification_type}) Enviando a {settings.ARTIFICIALIC_API_URL} "
                    f"para {phone_formatted} (Intento {attempt}/{max_retries})..."
                )
                response = await client.post(
                    url=settings.ARTIFICIALIC_API_URL,
                    json=payload,
                    headers=headers,
                )

                if response.status_code in (200, 201, 202):
                    logger.info(
                        f"📲 [NOTIFICADOR] [Task {task_id}] Notificación enviada con éxito (HTTP {response.status_code})"
                    )
                    await state_store.set_notification_status(task_id, sent=True)
                    return True

                # Errores candidatos a reintento: Rate limit (429) o fallos del servidor Artificialic (>=500)
                if response.status_code == 429 or response.status_code >= 500:
                    wait_sec = base_backoff * (2 ** (attempt - 1))
                    logger.warning(
                        f"📲 [NOTIFICADOR] [Task {task_id}] Fallo transitorio HTTP {response.status_code}: {response.text[:200]}. "
                        f"Reintentando en {wait_sec}s..."
                    )
                    if attempt < max_retries:
                        await asyncio.sleep(wait_sec)
                        continue
                    break

                # Errores deterministas de cliente (400, 401, 403, 404): No reintentar
                logger.error(
                    f"📲 [NOTIFICADOR] [Task {task_id}] Error 4xx irrecuperable en Artificialic ({response.status_code}): {response.text[:300]}"
                )
                break

            except (httpx.RequestError, httpx.TimeoutException) as net_err:
                wait_sec = base_backoff * (2 ** (attempt - 1))
                logger.warning(
                    f"📲 [NOTIFICADOR] [Task {task_id}] Error de red al contactar Artificialic: {net_err}. "
                    f"Reintentando en {wait_sec}s..."
                )
                if attempt < max_retries:
                    await asyncio.sleep(wait_sec)
                else:
                    logger.error(
                        f"📲 [NOTIFICADOR] [Task {task_id}] Agotados {max_retries} intentos de notificación hacia Artificialic."
                    )

            except Exception as unk_err:
                logger.error(f"📲 [NOTIFICADOR] [Task {task_id}] Error inesperado al notificar: {unk_err}")
                break

        # Si agotó intentos sin éxito
        await state_store.set_notification_status(task_id, sent=False)
        return False

    async def send_ticket_notification(
        self,
        telefono: str,
        ticket_id: Union[int, str],
        duration_seconds: float,
        task_id: str = "unknown",
    ) -> bool:
        """
        Envía o simula la notificación outbound cuando el ticket fue creado exitosamente en Odoo.
        """
        message_body = f"Tu ticket #{ticket_id} ha sido generado exitosamente."

        if settings.MOCK_ARTIFICIALIC:
            formatted_log = (
                f'📲 [MOCK NOTIFICACIÓN] Para: {telefono} | Mensaje: "{message_body}" '
                f"(Odoo latency: {duration_seconds:.2f}s)"
            )
            print(formatted_log, flush=True)
            logger.info(f"{formatted_log} [Task: {task_id}]")
            await state_store.set_notification_status(task_id, sent=True)
            return True

        return await self._dispatch_with_retry(
            telefono=telefono,
            message_body=message_body,
            task_id=task_id,
            notification_type="TICKET_OK",
        )

    async def send_failure_fallback_notification(
        self,
        telefono: str,
        order_name: str,
        task_id: str = "unknown",
    ) -> bool:
        """
        Envía un mensaje de contingencia al cliente cuando la creación en Odoo falló definitivamente.
        Garantiza que el usuario final no quede en el limbo.
        """
        fallback_msg = (
            f"Hemos registrado tu reporte para el contrato {order_name}. "
            f"En este momento nuestro sistema presenta alta demanda, por lo que un agente de soporte "
            f"procesará tu caso manualmente y te contactará a la brevedad."
        )

        if settings.MOCK_ARTIFICIALIC:
            formatted_log = (
                f'📲 [MOCK CONTINGENCIA] Para: {telefono} | Mensaje: "{fallback_msg}" [Task: {task_id}]'
            )
            print(formatted_log, flush=True)
            logger.info(f"{formatted_log}")
            await state_store.set_notification_status(task_id, sent=True)
            return True

        return await self._dispatch_with_retry(
            telefono=telefono,
            message_body=fallback_msg,
            task_id=task_id,
            notification_type="CONTINGENCIA_FALLO_ODOO",
        )


# Instancia singleton del notificador
notifier_client = NotifierClient()
