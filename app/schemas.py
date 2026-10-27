"""
Esquemas Pydantic para validación de datos, contratos de API y tipado estricto.
Incluye validación defensiva y coerción de tipos para integraciones externas (Artificialic).
"""

from datetime import datetime
from enum import Enum
import re
from typing import Any, Dict, Optional, Union
from pydantic import BaseModel, Field, field_validator


class TicketStatus(str, Enum):
    """Ciclo de vida y estados posibles de una tarea de ticket."""
    QUEUED = "queued"
    PROCESSING_ODOO = "processing_odoo"
    COMPLETED = "completed"
    FAILED = "failed"


class TicketEnqueueRequest(BaseModel):
    """Payload de entrada recibido en POST /api/tickets/enqueue."""
    telefono: str = Field(
        ...,
        min_length=5,
        max_length=30,
        description="Número de WhatsApp o teléfono del cliente",
        examples=["584123953003"],
    )
    order_name: str = Field(
        default="S/N",
        min_length=1,
        max_length=120,
        description="Código o identificador de la orden de venta",
        examples=["S05226"],
    )
    partner_id: int = Field(
        ...,
        description="ID del contacto / partner en Odoo",
        examples=[20959],
    )
    team_id: int = Field(
        default=1,
        description="ID del equipo de Helpdesk en Odoo",
        examples=[1],
    )
    ticket_comment: str = Field(
        default="Sin comentario adicional",
        description="Descripción o comentario para el ticket",
        examples=["Prueba"],
    )
    ticket_title: str = Field(
        default="",
        description="Título o asunto del ticket en Odoo",
        examples=["Título del Ticket"],
    )

    @field_validator("telefono", mode="before")
    @classmethod
    def clean_telefono(cls, v: Any) -> str:
        """Limpia espacios, guiones, paréntesis y signo '+' inicial de variables de WhatsApp."""
        if v is None:
            return ""
        s = str(v).strip()
        cleaned = re.sub(r"[\s\-\+\(\)\.]", "", s)
        return cleaned

    @field_validator("order_name", mode="before")
    @classmethod
    def clean_order_name(cls, v: Any) -> str:
        """
        Limpia espacios y garantiza que nunca se rechace un cliente sin orden activa.
        Retorna 'S/N' si es None, booleano False, o string vacío/'null'/'false'/'undefined'/'none'.
        """
        if v is None or v is False:
            return "S/N"
        s = str(v).strip()
        if not s or s.lower() in ("null", "false", "undefined", "none"):
            return "S/N"
        return s

    @field_validator("partner_id", mode="before")
    @classmethod
    def coerce_partner_id(cls, v: Any) -> int:
        """Coacciona partner_id a entero positivo mayor que cero limpiando espacios."""
        if v is None or v is False or isinstance(v, bool):
            raise ValueError("partner_id es requerido y no puede ser booleano ni nulo.")
        if isinstance(v, str):
            v_str = v.strip()
            if not v_str:
                raise ValueError("partner_id no puede estar vacío.")
            val = int(v_str)
        else:
            val = int(v)
        if val <= 0:
            raise ValueError("partner_id debe ser un entero positivo mayor que cero.")
        return val

    @field_validator("team_id", mode="before")
    @classmethod
    def coerce_team_id(cls, v: Any) -> int:
        """
        Coacciona el identificador del equipo de Helpdesk de forma tolerante:
        - None, vacío '', 'null', 'false', 'undefined' o 'soporte' -> 1 por defecto.
        - 'admin', 'administracion', 'administración' (case-insensitive) -> 3.
        - Numérico -> int(v).
        """
        if v is None or v is False:
            return 1
        s = str(v).strip().lower()
        if not s or s in ("null", "false", "undefined", "none", "soporte"):
            return 1
        if s in ("admin", "administracion", "administración"):
            return 3
        try:
            val = int(float(s)) if "." in s else int(s)
            return val if val > 0 else 1
        except (ValueError, TypeError):
            raise ValueError(f"team_id inválido: {v}")

    @field_validator("ticket_comment", mode="before")
    @classmethod
    def clean_ticket_comment(cls, v: Any) -> str:
        """Aplica strip y asigna valor por defecto si llega vacío o con solo espacios."""
        if v is None:
            return "Sin comentario adicional"
        s = str(v).strip()
        if not s:
            return "Sin comentario adicional"
        return s

    @field_validator("ticket_title", mode="before")
    @classmethod
    def clean_ticket_title(cls, v: Any) -> str:
        """Limpia espacios en ticket_title. Si es None o False, retorna string vacío."""
        if v is None or v is False:
            return ""
        return str(v).strip()

    def to_odoo_payload(self) -> Dict[str, Any]:
        """
        Genera estrictamente el payload esperado por el endpoint de Odoo,
        excluyendo campos externos como 'telefono'.
        """
        return {
            "order_name": self.order_name,
            "partner_id": self.partner_id,
            "team_id": self.team_id,
            "ticket_comment": self.ticket_comment,
            "ticket_title": self.ticket_title,
        }


class TicketEnqueueResponse(BaseModel):
    """Respuesta inmediata (HTTP 202 Accepted) entregada al cliente/chatbot."""
    status: TicketStatus = Field(
        default=TicketStatus.QUEUED,
        description="Estado inicial de la tarea",
        examples=["queued"],
    )
    task_id: str = Field(
        ...,
        min_length=12,
        max_length=12,
        description="Identificador hexadecimal único de 12 caracteres",
        examples=["a1b2c3d4e5f6"],
    )
    message: str = Field(
        default="Solicitud de ticket recibida y encolada exitosamente.",
        description="Mensaje informativo para el cliente o plataforma",
        examples=["Solicitud de ticket recibida y encolada exitosamente."],
    )


class TicketStatusResponse(BaseModel):
    """Respuesta detallada entregada en GET /api/tickets/status/{task_id}."""
    task_id: str = Field(..., description="ID único de la tarea")
    status: TicketStatus = Field(..., description="Estado actual de la tarea")
    ticket_id: Optional[Union[int, str]] = Field(
        default=None,
        description="ID del ticket generado en Odoo si la operación fue exitosa",
        examples=[15857],
    )
    duration_seconds: Optional[float] = Field(
        default=None,
        description="Tiempo total en segundos que tomó la transacción contra Odoo",
        examples=[2.45],
    )
    created_at: str = Field(
        ...,
        description="Timestamp ISO 8601 en UTC de recepción de la tarea",
        examples=["2026-10-20T13:20:00Z"],
    )
    error_detail: Optional[str] = Field(
        default=None,
        description="Detalle o traceback del error en caso de fallo",
        examples=[None],
    )
    notification_sent: Optional[bool] = Field(
        default=False,
        description="Indica si la notificación al cliente (o mensaje de contingencia) fue enviada",
        examples=[True],
    )


class QueueMetricsResponse(BaseModel):
    """Métricas en tiempo real consultables en GET /api/tickets/queue-metrics."""
    queued_tasks: int = Field(
        ...,
        ge=0,
        description="Cantidad actual de tareas pendientes en la cola en memoria/Redis",
    )
    active_workers: int = Field(
        ...,
        ge=0,
        description="Cantidad actual de workers ocupados procesando una tarea",
    )
    completed_total: int = Field(
        ...,
        ge=0,
        description="Total histórico de tareas completadas exitosamente",
    )
    failed_total: int = Field(
        ...,
        ge=0,
        description="Total histórico de tareas fallidas tras reintentos",
    )
