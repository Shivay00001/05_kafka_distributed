# 05_kafka_distributed - Event-Driven Architecture

> Production-grade Kafka-based distributed system demonstrating event sourcing and CQRS patterns.

## 🎯 Overview

This module implements:

- **Kafka Producer/Consumer** - Async message handling
- **Event Sourcing** - Event-based state management
- **CQRS** - Command Query Responsibility Segregation
- **Saga Pattern** - Distributed transactions
- **Dead Letter Queue** - Error handling

## 📁 Structure

```
05_kafka_distributed/
├── src/
│   ├── events/              # Event definitions
│   │   ├── base.py          # Base event class
│   │   └── domain_events.py # Domain events
│   ├── producers/           # Kafka producers
│   │   ├── base.py          # Base producer
│   │   └── order_producer.py# Order events
│   ├── consumers/           # Kafka consumers
│   │   ├── base.py          # Base consumer
│   │   └── order_consumer.py# Order processing
│   ├── handlers/            # Event handlers
│   ├── saga/                # Saga orchestration
│   └── dlq/                 # Dead letter queue
├── docker/                  # Docker configs
│   └── docker-compose.yml   # Kafka cluster
└── tests/                   # Test suite
```

## 🚀 Quick Start

```bash
# Start Kafka cluster
docker-compose -f docker/docker-compose.yml up -d

# Install dependencies
pip install -e .

# Run producer
python -m src.producers.order_producer

# Run consumer
python -m src.consumers.order_consumer
```

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      PRODUCERS                              │
│        OrderProducer │ InventoryProducer │ ...              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    KAFKA CLUSTER                            │
│            Topics │ Partitions │ Offsets                    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     CONSUMERS                               │
│       Consumer Groups │ Event Handlers │ Sagas              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                  DEAD LETTER QUEUE                          │
│            Failed Events │ Retry Logic                      │
└─────────────────────────────────────────────────────────────┘
```

## 📄 License

MIT
