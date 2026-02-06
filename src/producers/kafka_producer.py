"""
Kafka Producer - Async message producer with retry logic.
"""

import asyncio
from abc import ABC
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from tenacity import retry, stop_after_attempt, wait_exponential

from src.events.base import BaseEvent


@dataclass
class ProducerConfig:
    """Kafka producer configuration."""
    
    bootstrap_servers: str = "localhost:9092"
    client_id: str = "producer"
    acks: str = "all"
    retries: int = 3
    batch_size: int = 16384
    linger_ms: int = 10
    compression_type: str = "gzip"


class BaseProducer(ABC):
    """Base Kafka producer."""
    
    def __init__(
        self,
        config: Optional[ProducerConfig] = None,
        topic: str = "events",
    ):
        self.config = config or ProducerConfig()
        self.topic = topic
        self._producer = None
        self._callbacks: List[Callable] = []
    
    async def start(self) -> None:
        """Start the producer."""
        try:
            from aiokafka import AIOKafkaProducer
        except ImportError:
            raise ImportError("aiokafka required: pip install aiokafka")
        
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.config.bootstrap_servers,
            client_id=self.config.client_id,
            acks=self.config.acks,
            compression_type=self.config.compression_type,
        )
        await self._producer.start()
    
    async def stop(self) -> None:
        """Stop the producer."""
        if self._producer:
            await self._producer.stop()
    
    async def __aenter__(self):
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
    )
    async def send(
        self,
        event: BaseEvent,
        key: Optional[str] = None,
        partition: Optional[int] = None,
        headers: Optional[Dict[str, bytes]] = None,
    ) -> None:
        """
        Send an event to Kafka.
        
        Args:
            event: Event to send
            key: Optional partition key
            partition: Optional partition number
            headers: Optional message headers
        """
        if not self._producer:
            raise RuntimeError("Producer not started")
        
        message_key = (key or event.event_id).encode("utf-8")
        message_value = event.to_json()
        
        message_headers = []
        if headers:
            message_headers = [(k, v) for k, v in headers.items()]
        
        # Add standard headers
        message_headers.extend([
            ("event_type", event.event_type.encode("utf-8")),
            ("event_id", event.event_id.encode("utf-8")),
        ])
        
        await self._producer.send_and_wait(
            topic=self.topic,
            key=message_key,
            value=message_value,
            partition=partition,
            headers=message_headers,
        )
        
        # Call callbacks
        for callback in self._callbacks:
            await callback(event)
    
    async def send_batch(
        self,
        events: List[BaseEvent],
        key_func: Optional[Callable[[BaseEvent], str]] = None,
    ) -> None:
        """
        Send multiple events.
        
        Args:
            events: Events to send
            key_func: Function to generate partition key
        """
        for event in events:
            key = key_func(event) if key_func else None
            await self.send(event, key=key)
    
    def on_send(self, callback: Callable) -> None:
        """Register callback for successful sends."""
        self._callbacks.append(callback)


class OrderProducer(BaseProducer):
    """Producer for order-related events."""
    
    def __init__(self, config: Optional[ProducerConfig] = None):
        super().__init__(config, topic="orders")
    
    async def send_order_created(
        self,
        order_id: str,
        customer_id: str,
        customer_name: str,
        total_amount: float,
        currency: str = "USD",
    ) -> None:
        """Send OrderCreated event."""
        from src.events.base import OrderCreated
        
        event = OrderCreated(
            aggregate_id=order_id,
            customer_id=customer_id,
            customer_name=customer_name,
            total_amount=total_amount,
            currency=currency,
        )
        
        await self.send(event, key=order_id)
    
    async def send_order_confirmed(
        self,
        order_id: str,
        confirmed_by: Optional[str] = None,
    ) -> None:
        """Send OrderConfirmed event."""
        from src.events.base import OrderConfirmed
        
        event = OrderConfirmed(
            aggregate_id=order_id,
            confirmed_by=confirmed_by,
        )
        
        await self.send(event, key=order_id)
