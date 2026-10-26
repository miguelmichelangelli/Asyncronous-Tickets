# Quickbot Ticket Service

Microservicio asíncrono y distribuido de alto rendimiento para el desacoplamiento de la ingesta de tickets de soporte provenientes de WhatsApp (vía Artificialic SaaS) hacia el ERP Odoo, respaldado en **Redis Nativo** (`redis.asyncio`).

---

## 🏛️ Arquitectura del Sistema

El servicio opera bajo un patrón **Productor-Consumidor** desacoplado, garantizando respuestas HTTP 202 en menos de 50 ms al bot de WhatsApp mientras los workers absorben la alta latencia del ERP de forma resiliente:

```text
[ Artificialic / WhatsApp ]
           │
           │  POST /api/tickets/enqueue (< 50ms)
           ▼
┌────────────────────────────────────────────────────────┐
│ FastAPI API Gateway (Productor)                        │
│  - Sanitización de caracteres de control (\n, \r, \t)  │
│  - Candado atómico de idempotencia (60s)               │
│  - Registro en Hash con TTL 24h (ticket:task:{id})     │
│  - Encolado persistente RPUSH en ticket:queue          │
└────────────────────────────────────────────────────────┘
                           │
                           ▼
┌────────────────────────────────────────────────────────┐
│ Redis Nativo (Localhost 127.0.0.1:6379)                │
│  - Cola: ticket:queue (List)                           │
│  - En vuelo: ticket:processing (Set)                   │
│  - Estados: ticket:task:{id} (Hashes con TTL 24h)      │
│  - Candados: ticket:idemp:{hash} (String con TTL 60s)  │
│  - Métricas: ticket:metrics:completed|failed (Counters)│
└────────────────────────────────────────────────────────┘
                           │
                           │ BLPOP atómico (Workers)
                           ▼
┌────────────────────────────────────────────────────────┐
│ Pool de Workers Asíncronos (Consumidores)              │
│  - 5 workers concurrentes en segundo plano             │
│  - Llamada HTTP a Odoo con timeout de 300s             │
│  - Discriminación de errores y reintentos automáticos  │
│  - Recuperación de tareas huérfanas tras caída/reinicio│
└────────────────────────────────────────────────────────┘
           │                                │
           ▼                                ▼
   [ Odoo ERP API ]              [ Artificialic Outbound ]
  (Creación de Ticket)           (Confirmación por WhatsApp)
```

### Características de Resiliencia Empresarial
- **Persistencia y Auto-recuperación:** Las tareas en proceso se rastrean en el set `ticket:processing`. Si el servidor se apaga o reinicia (`systemctl restart`), al arrancar el servicio re-encola automáticamente las tareas interrumpidas hacia `ticket:queue`.
- **Idempotencia Inteligente:** Evita la duplicación de tickets en una ventana de 60 segundos por orden y cliente. Si una tarea previa falló, el candado se libera de inmediato permitiendo reintentos.
- **Prevención de Fugas de Memoria:** Todos los registros de tareas expiran automáticamente tras 24 horas (`TASK_TTL_SECONDS=86400`).
- **Tolerancia a Formatos Sucios:** Coerción automática de teléfonos (`+58 412...` -> `58412...`), códigos de orden vacíos (`S/N`), strings numéricos y saltos de línea literales en comentarios.
- **Notificación de Contingencia:** Si Odoo falla definitivamente, se envía automáticamente un WhatsApp de respaldo al cliente informándole que su requerimiento está en gestión.

---

## 🔌 Especificación de Endpoints

### 1. Ingesta de Tickets
- **Rutas:** `POST /api/tickets/enqueue` y `POST /api/tickets/enqueue/`
- **Código HTTP:** `202 Accepted`
- **Payload:**
```json
{
  "telefono": "584123953003",
  "order_name": "S05226",
  "partner_id": 20959,
  "team_id": 1,
  "ticket_comment": "Detalle del reporte o soporte"
}
```
- **Respuesta (Inmediata < 50ms):**
```json
{
  "status": "queued",
  "task_id": "a1b2c3d4e5f6",
  "message": "Solicitud de ticket recibida y encolada exitosamente."
}
```

### 2. Consulta de Estado
- **Ruta:** `GET /api/tickets/status/{task_id}`
- **Estados posibles:** `queued`, `processing_odoo`, `completed`, `failed`
- **Respuesta:**
```json
{
  "task_id": "a1b2c3d4e5f6",
  "status": "completed",
  "ticket_id": 15857,
  "duration_seconds": 1.45,
  "created_at": "2026-10-26T00:15:00Z",
  "error_detail": null,
  "notification_sent": true
}
```

### 3. Métricas Operativas
- **Ruta:** `GET /api/tickets/queue-metrics`
- **Respuesta:**
```json
{
  "queued_tasks": 0,
  "active_workers": 0,
  "completed_total": 450,
  "failed_total": 2,
  "status": "healthy"
}
```

### 4. Chequeo de Salud y Documentación
- **Health Check:** `GET /health` (valida estado de Redis, tamaño de cola y target Odoo).
- **Documentación Interactiva:** `GET /docs` (Swagger UI) y `GET /redoc`.

---

## ⚙️ Configuración (`.env`)

Copia la plantilla `.env.example` y define tus variables:

```bash
cp .env.example .env
```

| Variable | Tipo | Default | Descripción |
| :--- | :--- | :--- | :--- |
| `ODOO_BASE_URL` | `str` | `http://10.0.62.100:8069` | URL base del servidor Odoo |
| `ODOO_ENDPOINT` | `str` | `/quickbot_create_helpdesk_ticket/` | Ruta del endpoint en Odoo |
| `ODOO_TIMEOUT_SECONDS` | `float` | `300.0` | Timeout HTTP para Odoo (5 min) |
| `ODOO_MAX_RETRIES` | `int` | `2` | Reintentos ante fallos 5xx de Odoo |
| `SIMULATE_ODOO_DELAY_SECONDS`| `float`| `0.0` | Latencia artificial para tests de estrés |
| `WORKER_CONCURRENCY` | `int` | `5` | Número de workers concurrentes |
| `REDIS_URL` | `str` | `redis://127.0.0.1:6379/0` | URL del servicio Redis local |
| `REDIS_MAX_CONNECTIONS` | `int` | `20` | Pool máximo de conexiones Redis |
| `TASK_TTL_SECONDS` | `int` | `86400` | Expiración de Hashes de tareas (24h) |
| `IDEMPOTENCY_WINDOW_SECONDS` | `int` | `60` | Ventana de bloqueo para duplicados |
| `MOCK_ARTIFICIALIC` | `bool` | `False` | `True` para emular WhatsApp localmente |
| `ARTIFICIALIC_API_URL` | `str` | `https://artificialic.com/api/v1/messages` | Endpoint de mensajería |
| `ARTIFICIALIC_API_TOKEN` | `str` | `...` | Token Bearer de Artificialic |
| `SEND_FALLBACK_ON_ODOO_FAILURE`|`bool`| `True` | Disparar WhatsApp si Odoo falla |
| `APP_PORT` | `int` | `8000` | Puerto de escucha |

---

## 🚀 Ejecución Local

### Prerrequisitos
- Python 3.10+
- Servicio Redis activo en local (`sudo systemctl status redis` o `redis-server`)

### Instalación y Puesta en Marcha
```bash
# 1. Crear y activar entorno virtual
python -m venv venv
source venv/bin/activate  # En Windows: .\venv\Scripts\activate

# 2. Instalar dependencias
pip install -r requirements.txt

# 3. Iniciar el microservicio con Uvicorn
uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 1
```

---

## 🧪 Pruebas Unitarias y de Caos

La suite incluye 27 pruebas automatizadas que validan:
- Coerción de datos sucios y tipos.
- Caídas y reintentos de Odoo (503/500) y de Artificialic (502).
- Discriminación estricta de `ReadTimeout` (no reintentable).
- Auto-recuperación de tareas en vuelo ante `SIGTERM` / reinicios.
- Liberación del candado de idempotencia ante fallos.
- Soporte para saltos de línea literales en JSON.

Para ejecutar todas las pruebas:
```bash
pytest -v
```

---

## 🐧 Despliegue en Producción (Linux `systemd`)

Para desplegar en el servidor de producción en la ruta `/home/quickbot/software/quickbot-tickets`:

### 1. Clonar y Configurar Entorno
```bash
cd /home/quickbot/software/quickbot-tickets
python3 -m venv venv
./venv/bin/pip install --upgrade pip
./venv/bin/pip install -r requirements.txt
cp .env.example .env
# Configurar tokens y URLs en .env
```

### 2. Crear Unidad de Servicio Systemd
Crea el archivo `/etc/systemd/system/quickbot-tickets.service`:

```ini
[Unit]
Description=Quickbot Ticket Buffer Microservice (FastAPI + Redis)
After=network.target redis-server.service redis.service
Wants=redis.service

[Service]
Type=simple
User=quickbot
Group=quickbot
WorkingDirectory=/home/quickbot/software/quickbot-tickets
Environment="PATH=/home/quickbot/software/quickbot-tickets/venv/bin"
ExecStart=/home/quickbot/software/quickbot-tickets/venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 1 --log-level info

Restart=always
RestartSec=3
KillSignal=SIGTERM
TimeoutStopSec=30

LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

### 3. Activar e Iniciar Servicio
```bash
sudo systemctl daemon-reload
sudo systemctl enable quickbot-tickets
sudo systemctl start quickbot-tickets
sudo systemctl status quickbot-tickets
```

### 4. Monitoreo de Logs en Vivo
```bash
journalctl -u quickbot-tickets -f
```
