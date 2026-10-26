"""
Batería de pruebas de Ingeniería del Caos y Tolerancia a Fallos para Quickbot Ticket Service.
Valida:
1. Coerción de datos sucios provenientes de Artificialic/WhatsApp.
2. Caída transitoria de Odoo (HTTP 503) -> Reintento y éxito.
3. Caída total de Odoo -> Tarea en 'failed' y despacho automático de contingencia.
4. Fallo transitorio de Artificialic (HTTP 502 -> 200) -> Reintento y confirmación.
5. Auto-recuperación de tareas huérfanas en vuelo (ticket:processing) tras crash/reinicio.
"""

import asyncio
from unittest.mock import AsyncMock, patch
import fakeredis.aioredis
import httpx
import pytest

from app.clients.notifier_client import notifier_client
from app.clients.odoo_client import OdooClient, OdooClientError
from app.config import settings
from app.main import app
from app.queue_manager import (
    REDIS_PROCESSING_KEY,
    REDIS_QUEUE_KEY,
    queue_manager,
)
from app.redis_client import set_redis
from app.schemas import (
    TicketEnqueueRequest,
    TicketStatus,
)
from app.state_store import state_store


@pytest.fixture(autouse=True)
async def setup_fake_redis():
    """Inyecta una instancia limpia de FakeRedis para aislar cada prueba."""
    fake_r = fakeredis.aioredis.FakeRedis(decode_responses=True)
    set_redis(fake_r)
    yield fake_r
    await fake_r.flushall()
    await fake_r.aclose()


# ------------------------------------------------------------------------------
# 1. Test Coerción de Datos Sucios (Artificialic / WhatsApp)
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_data_coercion_dirty_input(setup_fake_redis):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        payload = {
            "telefono": " +58 412-3953003 ",
            "partner_id": " 20959 ",
            "team_id": " 1 ",
            "order_name": "  S05226  ",
            "ticket_comment": "    ",
        }

        # Comprobación a nivel de modelo Pydantic
        req = TicketEnqueueRequest(**payload)
        assert req.telefono == "584123953003"
        assert req.partner_id == 20959
        assert req.team_id == 1
        assert req.order_name == "S05226"
        assert req.ticket_comment == "Sin comentario adicional"

        # Comprobación a nivel de Endpoint HTTP (debe aceptar con 202)
        response = await ac.post("/api/tickets/enqueue", json=payload)
        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "queued"
        task_id = data["task_id"]

        # Verificar almacenamiento persistido limpio en Redis
        task_state = await state_store.get_task(task_id)
        assert task_state is not None
        assert task_state.task_id == task_id


# ------------------------------------------------------------------------------
# 2. Test Caída de Odoo (503 Service Unavailable) -> Reintento y Éxito
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_odoo_503_retry_and_success():
    client = OdooClient()

    resp_503 = httpx.Response(
        status_code=503,
        text="Service Unavailable (Odoo restart)",
        request=httpx.Request("POST", "http://test"),
    )
    resp_200 = httpx.Response(
        status_code=200,
        json={"result": {"result": {"ticket_id": 99123}}},
        request=httpx.Request("POST", "http://test"),
    )

    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post, \
         patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        # Intento 1: 503, Intento 2: 200
        mock_post.side_effect = [resp_503, resp_200]

        ticket_id, duration, _ = await client.create_ticket(
            payload={"order_name": "S05226", "partner_id": 20959, "team_id": 1, "ticket_comment": "Retry test"},
            task_id="chaos_retry_01",
        )

        assert ticket_id == 99123
        assert mock_post.call_count == 2
        assert mock_sleep.call_count == 1
    await client.close()


# ------------------------------------------------------------------------------
# 3. Test Caída Total de Odoo -> Tarea en 'failed' y Notificación de Contingencia
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_odoo_total_failure_triggers_fallback_notification(setup_fake_redis):
    fake_r = setup_fake_redis

    req = TicketEnqueueRequest(
        telefono="584123953003",
        order_name="S_FAIL_01",
        partner_id=20959,
        team_id=1,
        ticket_comment="Comentario falla",
    )
    task_id = "task_fail_999"
    await state_store.register_task(task_id, req)
    await queue_manager.enqueue(task_id)

    # Simular caída total de Odoo con ConnectError en todos los reintentos
    with patch("app.clients.odoo_client.odoo_client.create_ticket", new_callable=AsyncMock) as mock_odoo, \
         patch("app.config.settings.MOCK_ARTIFICIALIC", True), \
         patch.object(notifier_client, "send_failure_fallback_notification", wraps=notifier_client.send_failure_fallback_notification) as mock_fallback:

        mock_odoo.side_effect = OdooClientError("No fue posible conectar con Odoo tras agotar reintentos")

        # Iniciar worker momentáneamente para procesar la tarea encolada
        worker_task = asyncio.create_task(queue_manager._worker_loop(worker_id=1))
        queue_manager._is_running = True

        # Esperar brevemente a que el worker procese y dispare la contingencia
        for _ in range(20):
            task_info = await state_store.get_task(task_id)
            if task_info and task_info.status == TicketStatus.FAILED:
                break
            await asyncio.sleep(0.1)

        queue_manager._is_running = False
        worker_task.cancel()
        await asyncio.gather(worker_task, return_exceptions=True)

        # Validaciones de resiliencia
        final_state = await state_store.get_task(task_id)
        assert final_state.status == TicketStatus.FAILED
        assert "Odoo tras agotar reintentos" in (final_state.error_detail or "")
        assert mock_fallback.called
        assert final_state.notification_sent is True


# ------------------------------------------------------------------------------
# 4. Test Fallo Transitorio de Artificialic (HTTP 502 -> HTTP 200)
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_artificialic_transient_failure_exponential_backoff(setup_fake_redis):
    resp_502 = httpx.Response(
        status_code=502,
        text="Bad Gateway upstream",
        request=httpx.Request("POST", "http://test"),
    )
    resp_200 = httpx.Response(
        status_code=200,
        json={"status": "sent", "id": "msg_123"},
        request=httpx.Request("POST", "http://test"),
    )

    task_id = "notif_retry_01"
    req = TicketEnqueueRequest(
        telefono="584123953003",
        order_name="ORD_NOTIF",
        partner_id=20959,
        team_id=1,
        ticket_comment="Notif test",
    )
    await state_store.register_task(task_id, req)

    with patch("app.config.settings.MOCK_ARTIFICIALIC", False), \
         patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post, \
         patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:

        mock_post.side_effect = [resp_502, resp_200]

        success = await notifier_client.send_ticket_notification(
            telefono="584123953003",
            ticket_id=15857,
            duration_seconds=1.2,
            task_id=task_id,
        )

        assert success is True
        assert mock_post.call_count == 2
        # Backoff exponencial verificado (intento 1 -> duerme 2.0s)
        assert mock_sleep.call_count == 1
        mock_sleep.assert_called_with(settings.NOTIFIER_RETRY_BACKOFF_SECONDS)

        # Verificar que el Hash en Redis registra notification_sent: true
        task_data = await state_store.get_task(task_id)
        assert task_data.notification_sent is True


# ------------------------------------------------------------------------------
# 5. Test Auto-Recuperación de Tareas Huérfanas tras Crash (ticket:processing)
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_crash_recovery_orphaned_tasks(setup_fake_redis):
    fake_r = setup_fake_redis

    orphaned_id = "crash_task_888"
    req = TicketEnqueueRequest(
        telefono="584123953003",
        order_name="RECOVERY_01",
        partner_id=20959,
        team_id=1,
        ticket_comment="Tarea interrumpida por reinicio",
    )

    # Simular que el servidor se cayó: la tarea estaba registrada y en ticket:processing
    await state_store.register_task(orphaned_id, req)
    await state_store.set_processing(orphaned_id)
    await fake_r.sadd(REDIS_PROCESSING_KEY, orphaned_id)

    # Estado previo a la recuperación
    in_processing_before = await fake_r.smembers(REDIS_PROCESSING_KEY)
    assert orphaned_id in in_processing_before
    queue_len_before = await fake_r.llen(REDIS_QUEUE_KEY)
    assert queue_len_before == 0

    # Ejecutar rutina de recuperación que se corre al arrancar el servicio
    recovered_count = await queue_manager.recover_interrupted_tasks()
    assert recovered_count == 1

    # Estado posterior a la recuperación
    in_processing_after = await fake_r.smembers(REDIS_PROCESSING_KEY)
    assert orphaned_id not in in_processing_after

    # La tarea debe haber regresado a la cabeza de ticket:queue
    queue_len_after = await fake_r.llen(REDIS_QUEUE_KEY)
    assert queue_len_after == 1
    next_task_id = await fake_r.lpop(REDIS_QUEUE_KEY)
    assert next_task_id == orphaned_id

    # El estado de la tarea debe haber regresado a 'queued'
    task_info = await state_store.get_task(orphaned_id)
    assert task_info.status == TicketStatus.QUEUED


# ------------------------------------------------------------------------------
# 6. Test ReadTimeout Discrimination (No reintento para evitar duplicación)
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_odoo_read_timeout_no_retry():
    client = OdooClient()

    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        # Simular ReadTimeout tras enviar payload
        mock_post.side_effect = httpx.ReadTimeout("Server took too long to send response headers")

        with pytest.raises(OdooClientError) as exc_info:
            await client.create_ticket(
                payload={"order_name": "S_TIMEOUT", "partner_id": 20959, "team_id": 1, "ticket_comment": "Test"},
                task_id="task_read_timeout",
            )

        # Debe lanzar OdooClientError de inmediato sin hacer reintentos (call_count == 1)
        assert "ReadTimeout" in str(exc_info.value)
        assert "NO se reintenta" in str(exc_info.value)
        assert mock_post.call_count == 1
    await client.close()


# ------------------------------------------------------------------------------
# 7. Test Cancelación por SIGTERM durante llamada a Odoo (Preservación en ticket:processing)
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_worker_cancellation_during_odoo_call_preserves_task_for_recovery(setup_fake_redis):
    fake_r = setup_fake_redis

    task_id = "sigterm_during_odoo_01"
    req = TicketEnqueueRequest(
        telefono="584123953003",
        order_name="ORD_RESTART_TEST",
        partner_id=20959,
        team_id=1,
        ticket_comment="Prueba de reinicio abrupto con SIGTERM",
    )
    await state_store.register_task(task_id, req)
    await queue_manager.enqueue(task_id)

    odoo_started = asyncio.Event()
    odoo_blocker = asyncio.Event()

    async def mock_slow_odoo(payload, task_id):
        odoo_started.set()
        await odoo_blocker.wait()  # Espera indefinida hasta ser cancelado por SIGTERM
        return 99999, 1.0, {}

    queue_manager._is_running = True
    worker_task = asyncio.create_task(queue_manager._worker_loop(worker_id=10))

    with patch("app.queue_manager.odoo_client.create_ticket", side_effect=mock_slow_odoo), \
         patch("app.queue_manager.notifier_client.send_failure_fallback_notification", new_callable=AsyncMock) as mock_fallback:

        # Esperar a que el worker tome la tarea y entre en la llamada a Odoo
        await asyncio.wait_for(odoo_started.wait(), timeout=3.0)

        # Verificar que la tarea está activamente registrada en ticket:processing
        processing_mid_flight = await fake_r.smembers(REDIS_PROCESSING_KEY)
        assert task_id in processing_mid_flight

        # Simular SIGTERM / systemctl restart: cancelar worker en pleno vuelo
        queue_manager._is_running = False
        worker_task.cancel()
        await asyncio.gather(worker_task, return_exceptions=True)

        # VERIFICACIÓN CRÍTICA DEL FIX DE AUDITORÍA:
        # 1. La tarea NO debe haber sido eliminada de ticket:processing por el bloque finally:
        processing_after_cancel = await fake_r.smembers(REDIS_PROCESSING_KEY)
        assert task_id in processing_after_cancel, "¡El bloque finally: borró la tarea de ticket:processing!"

        # 2. La tarea NO debe haber sido marcada como FAILED ni haber disparado notificación de contingencia
        task_data = await state_store.get_task(task_id)
        assert task_data.status == TicketStatus.PROCESSING_ODOO
        assert task_data.error_detail is None
        assert not mock_fallback.called

        # 3. Simular que el servicio vuelve a arrancar: recover_interrupted_tasks() la recupera
        recovered = await queue_manager.recover_interrupted_tasks()
        assert recovered == 1

        # 4. Comprobar que regresó a la cola ticket:queue y su estado volvió a 'queued'
        processing_final = await fake_r.smembers(REDIS_PROCESSING_KEY)
        assert task_id not in processing_final

        queue_len = await fake_r.llen(REDIS_QUEUE_KEY)
        assert queue_len == 1
        requeued_id = await fake_r.lpop(REDIS_QUEUE_KEY)
        assert requeued_id == task_id

        task_after_recovery = await state_store.get_task(task_id)
        assert task_after_recovery.status == TicketStatus.QUEUED


# ------------------------------------------------------------------------------
# 8. Test Cancelación tras Éxito en Odoo (Evita duplicados al reiniciar)
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_worker_cancellation_after_odoo_success_does_not_duplicate_ticket(setup_fake_redis):
    fake_r = setup_fake_redis

    task_id = "sigterm_after_odoo_02"
    req = TicketEnqueueRequest(
        telefono="584123953003",
        order_name="ORD_ALREADY_DONE",
        partner_id=20959,
        team_id=1,
        ticket_comment="Prueba de tarea completada en Odoo antes de cancel",
    )
    await state_store.register_task(task_id, req)
    await queue_manager.enqueue(task_id)

    notif_started = asyncio.Event()
    notif_blocker = asyncio.Event()

    async def mock_slow_notif(*args, **kwargs):
        notif_started.set()
        await notif_blocker.wait()
        return True

    queue_manager._is_running = True
    worker_task = asyncio.create_task(queue_manager._worker_loop(worker_id=11))

    with patch("app.queue_manager.odoo_client.create_ticket", new_callable=AsyncMock) as mock_odoo, \
         patch("app.queue_manager.notifier_client.send_ticket_notification", side_effect=mock_slow_notif):

        mock_odoo.return_value = (77777, 0.8, {})

        # Esperar a que Odoo responda y el worker empiece a notificar
        await asyncio.wait_for(notif_started.wait(), timeout=3.0)

        # Cancelar worker en pleno paso de notificación
        queue_manager._is_running = False
        worker_task.cancel()
        await asyncio.gather(worker_task, return_exceptions=True)

        # Como Odoo ya completó exitosamente, el ticket #77777 ya existe
        task_data = await state_store.get_task(task_id)
        assert task_data.status == TicketStatus.COMPLETED
        assert task_data.ticket_id == 77777

        # Al reiniciar, recover_interrupted_tasks() detecta que ya completó y NO lo re-encola en ticket:queue
        recovered = await queue_manager.recover_interrupted_tasks()
        assert recovered == 0

        queue_len = await fake_r.llen(REDIS_QUEUE_KEY)
        assert queue_len == 0


# ------------------------------------------------------------------------------
# 9. Test Borde 1: Rechazo de ticket_id booleano False o cero en Odoo
# ------------------------------------------------------------------------------
def test_edge_odoo_boolean_false_ticket_id_rejected():
    from app.clients.odoo_client import extract_ticket_id

    # Odoo devuelve ticket_id: False en estructura anidada
    res_false = extract_ticket_id({"result": {"ticket_id": False}})
    assert res_false is None, f"Se esperaba None pero devolvió: {res_false}"

    # Odoo devuelve ticket_id: 0
    res_zero = extract_ticket_id({"ticket_id": 0})
    assert res_zero is None, f"Se esperaba None pero devolvió: {res_zero}"

    # Casos límite adicionales
    assert extract_ticket_id({"result": {"ticket_id": "false"}}) is None
    assert extract_ticket_id({"ticket_id": "0"}) is None
    assert extract_ticket_id({"ticket_id": None}) is None


# ------------------------------------------------------------------------------
# 10. Test Borde 2: Detección de Error Anidado en result de Odoo
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_edge_odoo_nested_error_detected():
    client = OdooClient()
    response_payload = {
        "jsonrpc": "2.0",
        "result": {
            "error": "Record does not exist",
        },
    }
    resp_200 = httpx.Response(
        status_code=200,
        json=response_payload,
        request=httpx.Request("POST", "http://test-odoo"),
    )

    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_post.return_value = resp_200
        with pytest.raises(OdooClientError) as exc_info:
            await client.create_ticket(
                payload={"order_name": "S_ERR", "partner_id": 20959, "team_id": 1, "ticket_comment": "Test"},
                task_id="task_nested_err",
            )
        assert "Record does not exist" in str(exc_info.value)
    await client.close()


# ------------------------------------------------------------------------------
# 11. Test Borde 3: Tolerancia a Saltos de Línea Literales en JSON
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_edge_raw_newlines_in_json_body(setup_fake_redis):
    # Payload con saltos de línea literales (\n en bytes, 0x0A) sin escapar dentro de un string
    raw_payload_bytes = (
        b'{\n'
        b'  "telefono": "584123953003",\n'
        b'  "order_name": "ORD_NEWLINE",\n'
        b'  "partner_id": 20959,\n'
        b'  "team_id": 1,\n'
        b'  "ticket_comment": "Linea 1 del cliente\nLinea 2 con detalles de pago\nLinea 3 final"\n'
        b'}'
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post(
            "/api/tickets/enqueue",
            content=raw_payload_bytes,
            headers={"Content-Type": "application/json"},
        )
        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "queued"
        assert "task_id" in data


# ------------------------------------------------------------------------------
# 12. Test Borde 4: Normalización de order_name vacío ('S/N') y team_id texto
# ------------------------------------------------------------------------------
def test_edge_empty_order_name_and_string_team_id():
    # order_name="" -> "S/N", team_id="ADMIN" -> 3
    req = TicketEnqueueRequest(
        telefono="584123953003",
        order_name="",
        partner_id=20959,
        team_id="ADMIN",
    )
    assert req.order_name == "S/N"
    assert req.team_id == 3

    # order_name="null" -> "S/N", partner_id con espacios, team_id="soporte" -> 1
    req2 = TicketEnqueueRequest(
        telefono="584123953003",
        order_name="null",
        partner_id=" 20959 ",
        team_id="soporte",
    )
    assert req2.order_name == "S/N"
    assert req2.partner_id == 20959
    assert req2.team_id == 1

    # order_name=None -> "S/N", team_id="administracion" -> 3
    req3 = TicketEnqueueRequest(
        telefono="584123953003",
        order_name=None,
        partner_id=10,
        team_id="administracion",
    )
    assert req3.order_name == "S/N"
    assert req3.team_id == 3


# ------------------------------------------------------------------------------
# 13. Test Borde 5: Tarea Fallida Libera Candado de Idempotencia
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_edge_failed_task_releases_idempotency_lock(setup_fake_redis):
    payload = {
        "telefono": "584123953003",
        "order_name": "ORD_IDEMP_FAIL",
        "partner_id": 20959,
        "team_id": 1,
        "ticket_comment": "Reintento tras fallo",
    }

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        # 1. Primera petición: se encola correctamente
        resp1 = await client.post("/api/tickets/enqueue", json=payload)
        assert resp1.status_code == 202
        task_id_1 = resp1.json()["task_id"]

        # 2. Si se reenvía inmediatamente mientras está en curso, se detecta duplicado
        resp_dup = await client.post("/api/tickets/enqueue", json=payload)
        assert resp_dup.status_code == 202
        assert resp_dup.json()["task_id"] == task_id_1
        assert "duplicada" in resp_dup.json()["message"]

        # 3. La tarea falla en Odoo
        await state_store.set_failed(
            task_id=task_id_1,
            error_detail="Simulación de fallo en Odoo",
        )

        # 4. El cliente reenvía la misma petición de inmediato:
        # El candado debe haber sido liberado y generar un nuevo task_id
        resp2 = await client.post("/api/tickets/enqueue", json=payload)
        assert resp2.status_code == 202
        task_id_2 = resp2.json()["task_id"]
        assert task_id_2 != task_id_1, f"Se esperaba nuevo task_id pero se reutilizó {task_id_1}"
        assert "exitosamente" in resp2.json()["message"]



