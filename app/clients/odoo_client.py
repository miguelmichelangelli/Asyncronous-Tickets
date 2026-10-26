"""
Cliente HTTP asíncrono robusto para interactuar con el ERP Odoo.
Maneja timeouts extendidos (300s), cabeceras de trazabilidad idempotentes (X-Idempotency-Key),
diferenciación de ReadTimeout (no reintentable) vs ConnectTimeout/5xx (reintentables con backoff lineal),
extracción recursiva y tolerante de 'ticket_id', y medición precisa de latencia.
"""

import asyncio
import logging
import re
import time
from typing import Any, Dict, Optional, Tuple, Union
import httpx

from app.config import settings

logger = logging.getLogger(__name__)


class OdooClientError(Exception):
    """Excepción base para errores en la comunicación con Odoo."""
    pass


class OdooRetryableError(OdooClientError):
    """Error transitorio de red o servidor 5xx candidato a reintento."""
    pass


class OdooExtractionError(OdooClientError):
    """Error cuando la respuesta no contiene un ticket_id válido."""
    pass


INVALID_TICKET_VALUES = {"", "false", "null", "none", "0"}


def _is_valid_ticket_id(val: Any) -> Optional[Union[int, str]]:
    """Valida y coacciona un identificador de ticket rechazando booleanos y valores nulos/cero."""
    if val is None or isinstance(val, bool):
        return None
    val_str = str(val).strip().lower()
    if val_str in INVALID_TICKET_VALUES:
        return None
    if isinstance(val, int) and val > 0:
        return val
    if val_str.isdigit() and int(val_str) > 0:
        return int(val_str)
    if isinstance(val, str) and str(val).strip():
        return str(val).strip()
    return None


def extract_odoo_error(data: Any) -> Optional[str]:
    """
    Verifica si existe un error reportado por Odoo en la raíz del diccionario
    o anidado dentro de 'result' (ej. {'jsonrpc': '2.0', 'result': {'error': 'Record does not exist'}}).
    Retorna el detalle del error como string si existe y es verdadero (distinto de False, None, '', {}).
    """
    if not isinstance(data, dict):
        return None

    def _get_truthy_error(val: Any) -> Optional[str]:
        if val is None or val is False:
            return None
        if isinstance(val, bool) and val is True:
            return "Error reportado por el servidor Odoo"
        if isinstance(val, (dict, list)):
            if len(val) == 0:
                return None
            return str(val)
        val_str = str(val).strip()
        if not val_str or val_str.lower() in ("false", "null", "none", "0"):
            return None
        return val_str

    # 1. Error en la raíz del diccionario (ej. {'error': {...}})
    if "error" in data:
        err = _get_truthy_error(data["error"])
        if err:
            return err

    # 2. Error dentro de 'result' (ej. {'result': {'error': '...'}})
    result_val = data.get("result")
    if isinstance(result_val, dict) and "error" in result_val:
        err = _get_truthy_error(result_val["error"])
        if err:
            return err

    return None


def extract_ticket_id(data: Any) -> Optional[Union[int, str]]:
    """
    Extrae de forma recursiva y tolerante el identificador 'ticket_id'
    desde cualquier nivel de anidación (dict, list) o estructura JSON-RPC típica.

    Rechaza explícitamente booleanos (False/True) y valores inválidos ("", "false", "null", "none", "0").
    """
    if data is None:
        return None

    if isinstance(data, dict):
        # 1. Búsqueda directa por clave 'ticket_id'
        if "ticket_id" in data:
            val = data["ticket_id"]
            if isinstance(val, bool):
                return None
            valid_id = _is_valid_ticket_id(val)
            if valid_id is not None:
                return valid_id
            return None

        # 2. Búsqueda en estructuras anidadas 'result'
        if "result" in data:
            nested = extract_ticket_id(data["result"])
            if nested is not None:
                return nested

        # 3. Búsqueda heurística en otras claves del dict
        for k, v in data.items():
            if k not in ("result", "ticket_id", "error"):
                res = extract_ticket_id(v)
                if res is not None:
                    return res

        # 4. Fallback: búsqueda por regex en campo 'message'
        if "message" in data and isinstance(data["message"], str):
            match = re.search(r"[Tt]icket\s*[:#]?\s*(\d+)", data["message"])
            if match:
                candidate = int(match.group(1))
                if candidate > 0:
                    return candidate

    elif isinstance(data, list):
        for item in data:
            res = extract_ticket_id(item)
            if res is not None:
                return res

    return None


class OdooClient:
    """Cliente HTTP asíncrono para Odoo ERP con gestión de ciclo de vida de conexión y trazabilidad."""

    def __init__(self):
        self._client: Optional[httpx.AsyncClient] = None

    def _build_headers(self, task_id: str = "unknown") -> Dict[str, str]:
        """Construye las cabeceras requeridas, parametrizadas y de trazabilidad idempotente."""
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Idempotency-Key": task_id,
            "X-Quickbot-Task-Id": task_id,
        }
        if settings.ODOO_HEADER_KEY_NAME and settings.ODOO_HEADER_KEY_VALUE:
            headers[settings.ODOO_HEADER_KEY_NAME] = settings.ODOO_HEADER_KEY_VALUE

        if settings.ODOO_HEADER_TOKEN_NAME and settings.ODOO_HEADER_TOKEN_VALUE:
            headers[settings.ODOO_HEADER_TOKEN_NAME] = settings.ODOO_HEADER_TOKEN_VALUE

        return headers

    async def get_client(self) -> httpx.AsyncClient:
        """Obtiene o inicializa el cliente HTTP asíncrono reutilizable con pool de conexiones."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(
                    timeout=settings.ODOO_TIMEOUT_SECONDS,
                    connect=30.0,
                    read=settings.ODOO_TIMEOUT_SECONDS,
                    write=30.0,
                ),
                limits=httpx.Limits(
                    max_keepalive_connections=20,
                    max_connections=50,
                ),
            )
        return self._client

    async def close(self) -> None:
        """Cierra el cliente HTTP liberando conexiones activas."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def create_ticket(
        self,
        payload: Dict[str, Any],
        task_id: str = "unknown",
    ) -> Tuple[Union[int, str], float, Dict[str, Any]]:
        """
        Envía la solicitud de creación de ticket a Odoo aplicando la política de reintentos inteligente.

        Diferencia estrictamente entre:
        - ReadTimeout: El payload llegó a Odoo, pero demoró la respuesta. NO se reintenta para evitar duplicación.
        - ConnectError / ConnectTimeout / WriteError / HTTP 502, 503, 504: Reintentables con backoff lineal.

        Args:
            payload: Dict estrictamente con keys 'order_name', 'partner_id', 'team_id', 'ticket_comment'
            task_id: Identificador de la tarea para trazabilidad e idempotencia en cabeceras

        Returns:
            Tuple con (ticket_id, duration_seconds, raw_response_json)

        Raises:
            OdooClientError: Cuando la llamada falla definitivamente tras agotar reintentos o recibir error fatal.
        """
        client = await self.get_client()
        url = settings.odoo_full_url
        headers = self._build_headers(task_id=task_id)
        max_retries = max(0, settings.ODOO_MAX_RETRIES)
        total_attempts = 1 + max_retries
        backoff_seconds = settings.ODOO_RETRY_BACKOFF_SECONDS

        overall_start_time = time.perf_counter()
        last_error_detail: Optional[str] = None

        for attempt in range(1, total_attempts + 1):
            attempt_start_time = time.perf_counter()
            logger.info(
                f"📡 [ODOO] [Task {task_id}] Enviando payload a {url} (Intento {attempt}/{total_attempts}) - "
                f"order={payload.get('order_name')}, partner={payload.get('partner_id')}"
            )

            try:
                # Inyección opcional de latencia simulada para pruebas de estrés y validación de buffers
                if settings.SIMULATE_ODOO_DELAY_SECONDS > 0:
                    logger.info(f"⏳ [SIMULACIÓN] Inyectando latencia artificial de {settings.SIMULATE_ODOO_DELAY_SECONDS}s...")
                    await asyncio.sleep(settings.SIMULATE_ODOO_DELAY_SECONDS)

                response = await client.post(
                    url=url,
                    json=payload,
                    headers=headers,
                )
                attempt_duration = time.perf_counter() - attempt_start_time

                # Verificar errores de servidor HTTP 5xx que califican para reintento (500, 502, 503, 504)
                if response.status_code >= 500:
                    error_msg = (
                        f"Odoo respondió con HTTP {response.status_code}: {response.text[:200]}"
                    )
                    logger.warning(
                        f"📡 [ODOO] [Task {task_id}] Fallo HTTP {response.status_code} en intento {attempt}/{total_attempts} "
                        f"({attempt_duration:.2f}s): {error_msg}"
                    )
                    last_error_detail = error_msg
                    if attempt < total_attempts:
                        logger.info(
                            f"📡 [ODOO] [Task {task_id}] Esperando {backoff_seconds}s de backoff lineal antes de reintentar..."
                        )
                        await asyncio.sleep(backoff_seconds)
                        continue
                    raise OdooClientError(
                        f"Servidor Odoo respondió con error {response.status_code} tras {total_attempts} intentos: {last_error_detail}"
                    )

                # Errores del cliente HTTP 4xx no deben reintentarse (error determinista)
                if 400 <= response.status_code < 500:
                    error_msg = (
                        f"Error de cliente HTTP {response.status_code} desde Odoo: {response.text[:300]}"
                    )
                    logger.error(f"📡 [ODOO] [Task {task_id}] Error 4xx irrecuperable: {error_msg}")
                    raise OdooClientError(error_msg)

                # Procesar respuesta exitosa (2xx)
                try:
                    response_json = response.json()
                except Exception as json_err:
                    error_msg = f"Respuesta de Odoo no es un JSON válido: {response.text[:200]}"
                    logger.error(f"📡 [ODOO] [Task {task_id}] {error_msg}")
                    raise OdooClientError(error_msg) from json_err

                # Verificar si JSON-RPC contiene error explícito (raíz o anidado en result)
                odoo_error = extract_odoo_error(response_json)
                if odoo_error:
                    error_msg = f"Error retornado por Odoo: {odoo_error}"
                    logger.error(f"📡 [ODOO] [Task {task_id}] {error_msg}")
                    raise OdooClientError(error_msg)

                # Extracción tolerante y recursiva del ticket_id
                ticket_id = extract_ticket_id(response_json)
                if ticket_id is None:
                    error_msg = (
                        f"No fue posible extraer 'ticket_id' de la respuesta de Odoo: {response_json}"
                    )
                    logger.error(f"📡 [ODOO] [Task {task_id}] {error_msg}")
                    raise OdooExtractionError(error_msg)

                total_duration = round(time.perf_counter() - overall_start_time, 2)
                logger.info(
                    f"📡 [ODOO] [Task {task_id}] Ticket #{ticket_id} extraído exitosamente "
                    f"en {total_duration:.2f}s (intento {attempt})"
                )
                return ticket_id, total_duration, response_json

            except httpx.ReadTimeout as read_err:
                # ReadTimeout: Odoo recibió la petición pero tardó más de ODOO_TIMEOUT_SECONDS en responder
                # NO se reintenta para evitar duplicación de tickets en el ERP
                attempt_duration = time.perf_counter() - attempt_start_time
                error_msg = (
                    f"ReadTimeout tras {attempt_duration:.2f}s esperando respuesta de Odoo. "
                    f"El payload ya fue despachado al servidor, por lo que NO se reintenta para evitar duplicidad."
                )
                logger.error(f"📡 [ODOO] [Task {task_id}] {error_msg}")
                raise OdooClientError(error_msg) from read_err

            except (httpx.ConnectError, httpx.ConnectTimeout, httpx.WriteError, httpx.PoolTimeout, httpx.NetworkError) as net_err:
                # Fallos de red donde la transacción NO se completó: reintentables
                attempt_duration = time.perf_counter() - attempt_start_time
                error_type = net_err.__class__.__name__
                last_error_detail = f"Fallo de conexión ({error_type}): {str(net_err)}"
                logger.warning(
                    f"📡 [ODOO] [Task {task_id}] Fallo de red en intento {attempt}/{total_attempts} "
                    f"tras {attempt_duration:.2f}s: {last_error_detail}"
                )

                if attempt < total_attempts:
                    logger.info(
                        f"📡 [ODOO] [Task {task_id}] Esperando {backoff_seconds}s de backoff lineal..."
                    )
                    await asyncio.sleep(backoff_seconds)
                else:
                    total_duration = round(time.perf_counter() - overall_start_time, 2)
                    final_msg = (
                        f"Agotados {total_attempts} intentos hacia Odoo tras {total_duration:.2f}s. "
                        f"Último error: {last_error_detail}"
                    )
                    logger.error(f"📡 [ODOO] [Task {task_id}] {final_msg}")
                    raise OdooClientError(final_msg) from net_err

            except (OdooClientError, OdooExtractionError):
                # Re-raise directo de errores identificados
                raise

            except Exception as unk_err:
                error_msg = f"Error inesperado al invocar Odoo: {unk_err.__class__.__name__}: {str(unk_err)}"
                logger.error(f"📡 [ODOO] [Task {task_id}] {error_msg}")
                raise OdooClientError(error_msg) from unk_err

        # Si agotó el loop sin retornar
        raise OdooClientError(
            f"Fallo al conectar con Odoo tras {total_attempts} intentos: {last_error_detail}"
        )


# Instancia singleton del cliente de Odoo
odoo_client = OdooClient()
