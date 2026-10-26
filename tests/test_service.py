"""
Batería de pruebas unitarias y de integración para Quickbot Ticket Service (Redis Native).
Valida contratos, extracción de ticket_id, reintentos, Redis Hashes, listas, TTL e Idempotencia.
"""

import asyncio
import hashlib
from unittest.mock import AsyncMock, patch
import fakeredis.aioredis
import httpx
import pytest
from pydantic import ValidationError

from app.clients.odoo_client import (
    OdooClient,
    OdooClientError,
    OdooExtractionError,
    extract_ticket_id,
)
from app.config import _clean_url_str, settings
from app.main import app
from app.queue_manager import queue_manager
from app.redis_client import set_redis
from app.schemas import (
    TicketEnqueueRequest,
    TicketEnqueueResponse,
    TicketStatus,
)
from app.state_store import state_store


# Fixture para inyectar FakeRedis en cada test
@pytest.fixture(autouse=True)
async def setup_fake_redis():
    fake_r = fakeredis.aioredis.FakeRedis(decode_responses=True)
    set_redis(fake_r)
    yield fake_r
    await fake_r.flushall()
    await fake_r.aclose()


# ------------------------------------------------------------------------------
# 1. Pruebas de Configuración y Schemas
# ------------------------------------------------------------------------------
def test_clean_url_str():
    assert _clean_url_str("[http://10.0.62.100:8069](http://10.0.62.100:8069)") == "http://10.0.62.100:8069"
    assert _clean_url_str("http://10.0.62.100:8069") == "http://10.0.62.100:8069"
    assert _clean_url_str(" 'http://localhost:8000' ") == "http://localhost:8000"


def test_schema_ticket_enqueue_request_valid():
    req = TicketEnqueueRequest(
        telefono="584123953003",
        order_name="S05226",
        partner_id=20959,
        team_id=1,
        ticket_comment="Prueba de comentario",
    )
    payload = req.to_odoo_payload()
    assert "telefono" not in payload
    assert payload == {
        "order_name": "S05226",
        "partner_id": 20959,
        "team_id": 1,
        "ticket_comment": "Prueba de comentario",
    }


def test_schema_ticket_enqueue_request_invalid():
    with pytest.raises(ValidationError):
        TicketEnqueueRequest(
            telefono="123",
            order_name="",
            ticket_comment="test",
        )


# ------------------------------------------------------------------------------
# 2. Pruebas de Extracción Tolerante y Recursiva de Ticket ID
# ------------------------------------------------------------------------------
def test_extract_ticket_id_nested_jsonrpc():
    payload = {
        "jsonrpc": "2.0",
        "id": None,
        "result": {
            "result": {
                "message": "Se ha generado el Ticket: 15857 con éxito!",
                "ticket_id": 15857,
            },
            "error": False,
        },
    }
    assert extract_ticket_id(payload) == 15857


def test_extract_ticket_id_single_nested():
    payload = {"result": {"ticket_id": 20450}}
    assert extract_ticket_id(payload) == 20450


def test_extract_ticket_id_root():
    payload = {"ticket_id": 9999}
    assert extract_ticket_id(payload) == 9999


def test_extract_ticket_id_string_conversion():
    payload = {"result": {"ticket_id": "8888"}}
    assert extract_ticket_id(payload) == 8888


def test_extract_ticket_id_fallback_regex():
    payload = {"result": {"message": "Se ha generado el Ticket: 77712 con éxito!"}}
    assert extract_ticket_id(payload) == 77712


def test_extract_ticket_id_not_found():
    payload = {"result": {"other_data": "none"}}
    assert extract_ticket_id(payload) is None


# ------------------------------------------------------------------------------
# 3. Pruebas de StateStore Respaldado en Redis Hashes
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_state_store_redis_hashes(setup_fake_redis):
    fake_r = setup_fake_redis
    req = TicketEnqueueRequest(
        telefono="584123953003",
        order_name="S001",
        partner_id=20959,
        team_id=1,
        ticket_comment="Test Redis",
    )

    # 1. Registro en Hash
    await state_store.register_task("task_red_01", req)

    # Validar que existe en Redis con TTL
    ttl = await fake_r.ttl("ticket:task:task_red_01")
    assert ttl > 0

    # 2. Lectura y deserialización
    task_resp = await state_store.get_task("task_red_01")
    assert task_resp is not None
    assert task_resp.task_id == "task_red_01"
    assert task_resp.status == TicketStatus.QUEUED

    # 3. Transición a processing_odoo
    await state_store.set_processing("task_red_01")
    updated = await state_store.get_task("task_red_01")
    assert updated.status == TicketStatus.PROCESSING_ODOO

    # 4. Transición a completed
    await state_store.set_completed("task_red_01", ticket_id=15857, duration_seconds=1.45)
    completed = await state_store.get_task("task_red_01")
    assert completed.status == TicketStatus.COMPLETED
    assert completed.ticket_id == 15857
    assert completed.duration_seconds == 1.45

    # 5. Métricas atómicas persistidas en Redis
    metrics = await state_store.get_metrics(queue_size=2)
    assert metrics.queued_tasks == 2
    assert metrics.completed_total == 1
    assert metrics.failed_total == 0

    # 6. Fallo
    await state_store.register_task("task_red_02", req)
    await state_store.set_failed("task_red_02", error_detail="Timeout Odoo")
    failed = await state_store.get_task("task_red_02")
    assert failed.status == TicketStatus.FAILED
    assert failed.error_detail == "Timeout Odoo"

    metrics2 = await state_store.get_metrics(queue_size=1)
    assert metrics2.failed_total == 1


# ------------------------------------------------------------------------------
# 4. Pruebas del Cliente Odoo, Retries y Latencia Simulada
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_odoo_client_success():
    client = OdooClient()
    mock_response = httpx.Response(
        status_code=200,
        json={"result": {"result": {"ticket_id": 12345}}},
        request=httpx.Request("POST", "http://test"),
    )

    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_post.return_value = mock_response
        ticket_id, duration, data = await client.create_ticket(
            payload={"order_name": "S001", "partner_id": 1, "team_id": 1, "ticket_comment": "OK"},
            task_id="abc123456789",
        )
        assert ticket_id == 12345
        assert duration >= 0.0
        assert mock_post.call_count == 1
    await client.close()


@pytest.mark.asyncio
async def test_odoo_client_simulated_delay():
    client = OdooClient()
    mock_response = httpx.Response(
        status_code=200,
        json={"result": {"ticket_id": 88888}},
        request=httpx.Request("POST", "http://test"),
    )

    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post, \
         patch("app.config.settings.SIMULATE_ODOO_DELAY_SECONDS", 0.05):
        mock_post.return_value = mock_response
        start = asyncio.get_event_loop().time()
        ticket_id, _, _ = await client.create_ticket(
            payload={"order_name": "S001", "partner_id": 1, "team_id": 1, "ticket_comment": "Delay"},
            task_id="delay_task",
        )
        elapsed = asyncio.get_event_loop().time() - start
        assert ticket_id == 88888
        assert elapsed >= 0.04
    await client.close()


@pytest.mark.asyncio
async def test_odoo_client_retries_on_500():
    client = OdooClient()
    mock_500 = httpx.Response(
        status_code=500,
        text="Internal Server Error",
        request=httpx.Request("POST", "http://test"),
    )
    mock_200 = httpx.Response(
        status_code=200,
        json={"result": {"ticket_id": 5555}},
        request=httpx.Request("POST", "http://test"),
    )

    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post, \
         patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        mock_post.side_effect = [mock_500, mock_200]
        ticket_id, duration, _ = await client.create_ticket(
            payload={"order_name": "S001", "partner_id": 1, "team_id": 1, "ticket_comment": "OK"},
            task_id="task_retry",
        )
        assert ticket_id == 5555
        assert mock_post.call_count == 2
        assert mock_sleep.call_count == 1
    await client.close()


# ------------------------------------------------------------------------------
# 5. Pruebas de Endpoints de FastAPI e Idempotencia en Redis
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_api_enqueue_and_idempotency(setup_fake_redis):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        req_payload = {
            "telefono": "584123953003",
            "order_name": "S05226",
            "partner_id": 20959,
            "team_id": 1,
            "ticket_comment": "Prueba de encolado Redis",
        }

        # 1. Primera Petición Legítima -> HTTP 202
        res1 = await ac.post("/api/tickets/enqueue", json=req_payload)
        assert res1.status_code == 202
        data1 = res1.json()
        task_id_1 = data1["task_id"]
        assert len(task_id_1) == 12
        assert "exitosamente" in data1["message"]

        # 2. Segunda Petición Idéntica (Idempotencia) -> Mismo Task ID, mensaje informativo
        res2 = await ac.post("/api/tickets/enqueue", json=req_payload)
        assert res2.status_code == 202
        data2 = res2.json()
        assert data2["task_id"] == task_id_1
        assert "duplicada" in data2["message"].lower()

        # 3. Consultar Estado en Redis
        status_res = await ac.get(f"/api/tickets/status/{task_id_1}")
        assert status_res.status_code == 200
        assert status_res.json()["task_id"] == task_id_1

        # 4. Consultar Métricas
        metrics_res = await ac.get("/api/tickets/queue-metrics")
        assert metrics_res.status_code == 200
        metrics = metrics_res.json()
        assert "queued_tasks" in metrics
        assert "completed_total" in metrics
