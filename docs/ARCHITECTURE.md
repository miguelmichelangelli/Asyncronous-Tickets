# ARQUITECTURA Y CONTEXTO DEL SISTEMA: BUFFER ASÍNCRONO DE TICKETS

## 1. Problema de Negocio y Causa Raíz
El chatbot de atención al cliente (operando sobre el SaaS Artificialic / Meta WhatsApp) genera tickets en Odoo ERP de forma síncrona mediante peticiones HTTP.
Durante incidentes o cambios en producción, el endpoint de creación de tickets de Odoo experimentó latencias de más de 4 minutos (240+ segundos).

Consecuencia:
Los nodos HTTP de plataformas SaaS y proxies tienen límites de espera estrictos (timeouts de 10 a 30 segundos). Al vencerse este plazo, el chatbot aborta la conexión e informa falsamente al cliente que ocurrió un error, a pesar de que el proceso transaccional continúa ejecutándose internamente en Odoo.

## 2. Solución Arquitectónica: Patrón Productor-Consumidor (Buffer Asíncrono)
Para desvincular la experiencia del usuario de la latencia del ERP, se implementa un microservicio intermedio en Python/FastAPI:

1. Ingesta Rápida (Productor):
   El chatbot envía la solicitud de ticket al endpoint `/api/tickets/enqueue`.
   El microservicio valida la estructura, genera un `task_id` determinista, encola la tarea en memoria (`asyncio.Queue`) y responde de inmediato con `HTTP 202 Accepted` (< 50 ms).
   El cliente recibe confirmación instantánea en WhatsApp: "Tu solicitud fue recibida y se está procesando".

2. Procesamiento Desacoplado (Consumidor / Workers):
   Un pool de workers asíncronos en segundo plano toma las solicitudes de la cola de forma ordenada (FIFO) con control de concurrencia (`WORKER_CONCURRENCY`).
   El worker ejecuta la petición HTTP pesada contra Odoo con un timeout generoso (300 s) y tolerancia a fallos. Si Odoo tarda 4 minutos, la conversación de WhatsApp no se bloquea.

3. Notificación Proactiva (Outbound Callback):
   Al confirmarse la creación del ticket en Odoo con su `ticket_id`, el worker consume la API de Artificialic para inyectar un mensaje proactivo al cliente con el número asignado.

## 3. Estado de la Tarea (Lifecycle)
- `queued`: Tarea recibida en el microservicio, esperando turno en la cola.
- `processing_odoo`: Worker ejecutando la transacción contra el ERP Odoo.
- `completed`: Ticket creado exitosamente en Odoo y notificación procesada.
- `failed`: Transacción fallida tras agotar los reintentos permitidos.