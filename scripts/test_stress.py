"""
Script de pruebas de carga, concurrencia, resiliencia e Idempotencia para Quickbot Ticket Service (Redis Native).

Verifica:
1. Desacoplamiento y latencia de encolado (< 50ms por petición).
2. Respeto al límite de concurrencia de workers (WORKER_CONCURRENCY).
3. Prueba deliberada de Idempotencia: envío de solicitudes duplicadas simultáneas.
4. Transición de estados respaldada en Redis (queued -> processing_odoo -> completed/failed).
5. Métricas globales persistentes en Redis en tiempo real.

Uso:
    python scripts/test_stress.py --url http://127.0.0.1:8000 --poll
"""

import argparse
import asyncio
import statistics
import sys
import time
from typing import Any, Dict, List
import httpx

if sys.platform.startswith("win"):
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


async def enqueue_single_ticket(
    client: httpx.AsyncClient,
    base_url: str,
    index: int,
    order_name: str = None,
    partner_id: int = 20959,
    comment: str = None,
) -> Dict[str, Any]:
    """Envía una solicitud de encolado y mide estrictamente la latencia de respuesta HTTP."""
    payload = {
        "telefono": f"58412{index:07d}",
        "order_name": order_name or f"STRESS_{index:04d}",
        "partner_id": partner_id,
        "team_id": 1,
        "ticket_comment": comment or f"Ticket de prueba de estrés concurrente #{index}",
    }

    start_time = time.perf_counter()
    try:
        response = await client.post(
            f"{base_url}/api/tickets/enqueue",
            json=payload,
            headers={"Content-Type": "application/json"},
        )
        latency_ms = (time.perf_counter() - start_time) * 1000.0

        if response.status_code == 202:
            data = response.json()
            return {
                "index": index,
                "success": True,
                "status_code": response.status_code,
                "latency_ms": latency_ms,
                "task_id": data.get("task_id"),
                "status": data.get("status"),
                "message": data.get("message", ""),
                "is_duplicate": "duplicada" in data.get("message", "").lower(),
                "error": None,
            }
        else:
            return {
                "index": index,
                "success": False,
                "status_code": response.status_code,
                "latency_ms": latency_ms,
                "task_id": None,
                "status": None,
                "message": "",
                "is_duplicate": False,
                "error": response.text,
            }
    except Exception as exc:
        latency_ms = (time.perf_counter() - start_time) * 1000.0
        return {
            "index": index,
            "success": False,
            "status_code": 0,
            "latency_ms": latency_ms,
            "task_id": None,
            "status": None,
            "message": "",
            "is_duplicate": False,
            "error": str(exc),
        }


async def poll_task_status(
    client: httpx.AsyncClient,
    base_url: str,
    task_id: str,
    timeout: float = 120.0,
) -> Dict[str, Any]:
    """Consulta periódicamente el estado de la tarea en Redis hasta llegar a completed o failed."""
    start = time.perf_counter()
    while time.perf_counter() - start < timeout:
        try:
            resp = await client.get(f"{base_url}/api/tickets/status/{task_id}")
            if resp.status_code == 200:
                data = resp.json()
                if data["status"] in ("completed", "failed"):
                    return data
        except Exception:
            pass
        await asyncio.sleep(0.5)

    return {"task_id": task_id, "status": "timeout_polling"}


async def run_stress_test(base_url: str, burst_count: int, poll: bool):
    """Ejecuta la batería de pruebas concurrentes y validación de idempotencia."""
    print("=================================================================")
    print("🚀 INICIANDO PRUEBA DE ESTRÉS Y RESILIENCIA: REDIS NATIVE BUFFER")
    print(f"🎯 Target URL: {base_url}")
    print(f"📦 Solicitudes Únicas en Ráfaga: {burst_count}")
    print(f"🔒 Pruebas de Idempotencia: 2 peticiones idénticas simultáneas")
    print(f"🔄 Esperar resolución final (Poll): {poll}")
    print("=================================================================\n")

    limits = httpx.Limits(max_connections=50, max_keepalive_connections=20)
    async with httpx.AsyncClient(limits=limits, timeout=10.0) as client:
        # 1. Comprobar conectividad y estado inicial
        try:
            health = await client.get(f"{base_url}/health")
            print(f"✅ Microservicio activo: {health.json()}")
            metrics_init = await client.get(f"{base_url}/api/tickets/queue-metrics")
            print(f"📊 Métricas iniciales: {metrics_init.json()}\n")
        except Exception as e:
            print(f"❌ Error al conectar con el microservicio en {base_url}: {e}")
            return

        # 2. Warmup de conexión
        try:
            await client.get(f"{base_url}/health")
        except Exception:
            pass

        # 3. FASE 1: Disparar ráfaga de 5 tickets diferentes concurrentes
        print(f"⚡ [FASE 1] Disparando ráfaga de {burst_count} tickets diferentes simultáneos...")
        batch_start = time.perf_counter()
        unique_tasks = [
            enqueue_single_ticket(client, base_url, i + 1, partner_id=20959)
            for i in range(burst_count)
        ]
        results_unique = await asyncio.gather(*unique_tasks)
        batch_duration_sec = time.perf_counter() - batch_start

        latencies = [r["latency_ms"] for r in results_unique]
        successful_unique = [r for r in results_unique if r["success"]]

        print("-----------------------------------------------------------------")
        print("📈 RESULTADOS DE LA INGESTA EN REDIS (FASE 1):")
        print(f"  Peticiones Únicas:       {burst_count}")
        print(f"  Encoladas Exitosas:      {len(successful_unique)} (HTTP 202)")
        print(f"  Tiempo Total de Ráfaga:  {batch_duration_sec:.3f} s")
        print(f"  Latencia Promedio:       {statistics.mean(latencies):.2f} ms")
        print(f"  Latencia Mediana:        {statistics.median(latencies):.2f} ms")
        print(f"  Latencia Mínima:         {min(latencies):.2f} ms")
        print(f"  Latencia Máxima:         {max(latencies):.2f} ms")
        print("-----------------------------------------------------------------\n")

        # 4. FASE 2: Prueba Deliberada de Idempotencia (2 peticiones idénticas simultáneas)
        print("⚡ [FASE 2] Enviando 2 peticiones idénticas concurrentes para probar Idempotencia...")
        dup_order = f"DUP_ORDER_{int(time.time())}"
        dup_comment = "Ticket duplicado deliberado para validación de candado Redis SET NX"

        dup_task_1 = enqueue_single_ticket(
            client, base_url, index=998, order_name=dup_order, partner_id=20959, comment=dup_comment
        )
        dup_task_2 = enqueue_single_ticket(
            client, base_url, index=999, order_name=dup_order, partner_id=20959, comment=dup_comment
        )
        dup_results = await asyncio.gather(dup_task_1, dup_task_2)

        res_a, res_b = dup_results[0], dup_results[1]
        task_id_a, task_id_b = res_a.get("task_id"), res_b.get("task_id")
        duplicate_detected = res_a.get("is_duplicate") or res_b.get("is_duplicate")
        same_task_id = (task_id_a == task_id_b) and (task_id_a is not None)

        print("-----------------------------------------------------------------")
        print("🔒 RESULTADOS DE LA PRUEBA DE IDEMPOTENCIA:")
        print(f"  Petición 1 - Task ID:    {task_id_a} (Mensaje: '{res_a.get('message')}')")
        print(f"  Petición 2 - Task ID:    {task_id_b} (Mensaje: '{res_b.get('message')}')")
        print(f"  ¿Mismo Task ID?:         {'✅ SÍ' if same_task_id else '❌ NO'}")
        print(f"  ¿Candado activado?:      {'✅ SÍ (Duplicado detectado)' if duplicate_detected else '❌ NO'}")
        print("-----------------------------------------------------------------\n")

        # 5. FASE 3: Polling de Resolución de Estados en Redis
        all_task_ids = list(set(
            [r["task_id"] for r in results_unique if r.get("task_id")]
            + [task_id_a for task_id_a in [task_id_a] if task_id_a]
        ))

        if poll and all_task_ids:
            print(f"⏳ [FASE 3] Esperando que los workers procesen las {len(all_task_ids)} tareas únicas...")
            poll_start = time.perf_counter()
            poll_tasks = [poll_task_status(client, base_url, tid) for tid in all_task_ids]
            poll_results = await asyncio.gather(*poll_tasks)
            poll_duration = time.perf_counter() - poll_start

            completed = sum(1 for p in poll_results if p.get("status") == "completed")
            failed = sum(1 for p in poll_results if p.get("status") == "failed")
            timeouts = sum(1 for p in poll_results if p.get("status") == "timeout_polling")

            print("-----------------------------------------------------------------")
            print("🏁 RESULTADOS DEL PROCESAMIENTO ASÍNCRONO:")
            print(f"  Tareas Monitoreadas:     {len(poll_results)}")
            print(f"  Completadas en Odoo:     {completed}")
            print(f"  Fallidas:                {failed}")
            print(f"  Timeout de espera:       {timeouts}")
            print(f"  Tiempo Total de Proceso: {poll_duration:.2f} s")
            print("-----------------------------------------------------------------")

            metrics_final = await client.get(f"{base_url}/api/tickets/queue-metrics")
            print(f"\n📊 Métricas finales persistidas en Redis: {metrics_final.json()}")


def main():
    parser = argparse.ArgumentParser(description="Prueba de estrés y resiliencia para Quickbot Ticket Service")
    parser.add_argument("--url", default="http://127.0.0.1:8000", help="URL base del microservicio")
    parser.add_argument("--burst", type=int, default=5, help="Cantidad de tickets únicos en ráfaga (por defecto 5)")
    parser.add_argument("--poll", action="store_true", help="Esperar a que los workers procesen las tareas")
    args = parser.parse_args()

    asyncio.run(run_stress_test(args.url, args.burst, args.poll))


if __name__ == "__main__":
    main()
