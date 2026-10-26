# ESPECIFICACIÓN DE CONTRATOS DE API Y DATOS

## 1. Ingesta: `POST /api/tickets/enqueue`
Endpoint consumido por el Chatbot, Swagger UI o Postman.

- **Headers:** `Content-Type: application/json`
- **Payload Request:**
```json
{
  "telefono": "584123953003",
  "order_name": "S05226",
  "partner_id": 20959,
  "team_id": 1,
  "ticket_comment": "Prueba"
}

Respuesta Inmediata (202 Accepted):
{
  "status": "queued",
  "task_id": "a1b2c3d4e5f6",
  "message": "Solicitud de ticket recibida y encolada exitosamente."
}

2. Salida hacia ERP Odoo (Instancia de Pruebas)
URL Base: http://10.0.62.100:8069

Endpoint: /quickbot_create_helpdesk_ticket/ (respetar trailing slash)

Método: POST

Headers:

Content-Type: application/json

Cabeceras de autenticación parametrizables por entorno (ODOO_HEADER_KEY_NAME: ODOO_HEADER_KEY_VALUE y ODOO_HEADER_TOKEN_NAME: ODOO_HEADER_TOKEN_VALUE).

Payload hacia Odoo
{
  "order_name": "S05226",
  "partner_id": 20959,
  "team_id": 1,
  "ticket_comment": "Prueba"
}

Estructura de Respuesta Típica de Odoo (JSON-RPC / Estándar):
{
  "jsonrpc": "2.0",
  "id": null,
  "result": {
    "result": {
      "message": "Se ha generado el Ticket: 15857 con éxito!",
      "ticket_id": 15857
    },
    "error": false
  }
}

3. Consulta de Estado: GET /api/tickets/status/{task_id}
Respuesta (200 OK):

JSON
{
  "task_id": "a1b2c3d4e5f6",
  "status": "completed",
  "ticket_id": 15857,
  "duration_seconds": 2.45,
  "created_at": "2026-10-20T13:20:00Z",
  "error_detail": null
}
4. Métricas de Cola: GET /api/tickets/queue-metrics
Respuesta (200 OK):

JSON
{
  "queued_tasks": 0,
  "active_workers": 0,
  "completed_total": 1,
  "failed_total": 0
}