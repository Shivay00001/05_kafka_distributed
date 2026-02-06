"""
Base Event - Foundation for all domain events.
"""

from abc import ABC
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import uuid4

import orjson


@dataclass
class BaseEvent(ABC):
    """
    Base class for all domain events.
    
    All events are immutable and contain metadata
    for tracking and auditing.
    """
    
    event_id: str = field(default_factory=lambda: str(uuid4()))
    event_type: str = field(default="")
    timestamp: datetime = field(default_factory=datetime.utcnow)
    version: int = field(default=1)
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        if not self.event_type:
            self.event_type = self.__class__.__name__
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary."""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "timestamp": self.timestamp.isoformat(),
            "version": self.version,
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "metadata": self.metadata,
            "payload": self._get_payload(),
        }
    
    def _get_payload(self) -> Dict[str, Any]:
        """Get event-specific payload."""
        payload = {}
        for key, value in self.__dict__.items():
            if key not in {"event_id", "event_type", "timestamp", "version",
                          "correlation_id", "causation_id", "metadata"}:
                if isinstance(value, datetime):
                    payload[key] = value.isoformat()
                else:
                    payload[key] = value
        return payload
    
    def to_json(self) -> bytes:
        """Serialize event to JSON bytes."""
        return orjson.dumps(self.to_dict())
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BaseEvent":
        """Create event from dictionary."""
        payload = data.pop("payload", {})
        data.update(payload)
        
        if "timestamp" in data and isinstance(data["timestamp"], str):
            data["timestamp"] = datetime.fromisoformat(data["timestamp"])
        
        return cls(**data)
    
    @classmethod
    def from_json(cls, json_bytes: bytes) -> "BaseEvent":
        """Deserialize event from JSON bytes."""
        data = orjson.loads(json_bytes)
        return cls.from_dict(data)


@dataclass
class DomainEvent(BaseEvent):
    """Base class for domain-specific events."""
    
    aggregate_id: str = ""
    aggregate_type: str = ""
    
    def _get_payload(self) -> Dict[str, Any]:
        payload = super()._get_payload()
        payload["aggregate_id"] = self.aggregate_id
        payload["aggregate_type"] = self.aggregate_type
        return payload


# Order Events
@dataclass
class OrderCreated(DomainEvent):
    """Event when a new order is created."""
    
    customer_id: str = ""
    customer_name: str = ""
    total_amount: float = 0.0
    currency: str = "USD"
    
    def __post_init__(self):
        super().__post_init__()
        self.aggregate_type = "Order"


@dataclass
class OrderConfirmed(DomainEvent):
    """Event when an order is confirmed."""
    
    confirmed_by: Optional[str] = None
    
    def __post_init__(self):
        super().__post_init__()
        self.aggregate_type = "Order"


@dataclass
class OrderShipped(DomainEvent):
    """Event when an order is shipped."""
    
    tracking_number: Optional[str] = None
    carrier: Optional[str] = None
    estimated_delivery: Optional[str] = None
    
    def __post_init__(self):
        super().__post_init__()
        self.aggregate_type = "Order"


@dataclass
class OrderCancelled(DomainEvent):
    """Event when an order is cancelled."""
    
    reason: str = ""
    cancelled_by: Optional[str] = None
    
    def __post_init__(self):
        super().__post_init__()
        self.aggregate_type = "Order"


# Inventory Events
@dataclass
class InventoryReserved(DomainEvent):
    """Event when inventory is reserved."""
    
    order_id: str = ""
    sku: str = ""
    quantity: int = 0
    
    def __post_init__(self):
        super().__post_init__()
        self.aggregate_type = "Inventory"


@dataclass
class InventoryReleased(DomainEvent):
    """Event when reserved inventory is released."""
    
    order_id: str = ""
    sku: str = ""
    quantity: int = 0
    
    def __post_init__(self):
        super().__post_init__()
        self.aggregate_type = "Inventory"
