# 05_kafka_distributed

> Distributed event-driven system built on Kafka for asynchronous communication, domain events, retry workflows, and resilient processing.

[![Python](https://img.shields.io/badge/Python-3.11%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Kafka](https://img.shields.io/badge/Kafka-Event%20Streaming-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-Custom%20Commercial-orange)](./LICENSE)

This repository is a distributed event-driven foundation built around Apache Kafka. It models asynchronous communication between producers and consumers, domain event handling, workflow orchestration, and failure isolation through dead-letter patterns.

It is intended for systems that need reliable ingestion, event-based processing, loosely coupled services, and asynchronous workflows across business domains.

## What this project includes

- Kafka producer and consumer patterns
- event-driven architecture foundations
- domain event modeling
- command/query segregation concepts
- saga-style workflow orchestration
- dead-letter queue and retry concerns
- local Kafka cluster setup through Docker

## Repository structure

```text
05_kafka_distributed/
├── src/
│   ├── events/
│   │   ├── base.py
│   │   └── domain_events.py
│   ├── producers/
│   │   ├── base.py
│   │   └── order_producer.py
│   ├── consumers/
│   │   ├── base.py
│   │   └── order_consumer.py
│   ├── handlers/
│   ├── saga/
│   ├── dlq/
│   └── main.py
├── docker/
│   └── docker-compose.yml
├── tests/
├── README.md
├── LICENSE
├── pyproject.toml
├── .env.example
├── .gitignore
└── Dockerfile
```

## System goals

This project is designed to support distributed processing and event-driven microservice patterns, including:

- decoupling producers from consumers
- asynchronous updates across systems
- event-driven integration between business modules
- retry and failure isolation for unreliable workflows
- domain event propagation for eventual consistency

## Architecture overview

```text
┌───────────────────────────────────────────────────────────────────┐
│                         Producers                                  │
│ order events │ inventory events │ workflow events                  │
└───────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌───────────────────────────────────────────────────────────────────┐
│                        Kafka Cluster                              │
│ topics │ partitions │ offsets │ retention │ consumer groups        │
└───────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌───────────────────────────────────────────────────────────────────┐
│                         Consumers                                  │
│ handlers │ state updates │ downstream processing                 │
└───────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌───────────────────────────────────────────────────────────────────┐
│                        Dead Letter Queue                          │
│ failed events │ retry queue │ operational inspection             │
└───────────────────────────────────────────────────────────────────┘
```

## Core capabilities

This project is useful for:

- event-driven integration layers
- distributed order processing
- async task orchestration
- event sourcing and replay scenarios
- service-to-service messaging patterns
- workflow reliability with failure isolation

## Quick start

### Prerequisites

- Docker and Docker Compose
- Python 3.11+
- Kafka-compatible environment for local or cloud deployment

### Start Kafka locally

```bash
docker-compose -f docker/docker-compose.yml up -d
```

### Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -e .
```

### Configure environment

```bash
cp .env.example .env
```

Example:

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_CLIENT_ID=distributed-events
KAFKA_SECURITY_PROTOCOL=PLAINTEXT
TOPIC_NAME=orders.events
GROUP_ID=order-consumer-group
```

### Run producer and consumer

```bash
python -m src.producers.order_producer
python -m src.consumers.order_consumer
```

## Production-readiness assessment

### Current maturity: strong distributed systems foundation

This repo is a good architectural foundation for event-driven systems, but it should still be treated as a starter implementation rather than a complete production platform.

### Strengths

- clear event-driven design
- Kafka-first asynchronous communication model
- modular producer/consumer structure
- patterns for failure handling and workflow segmentation
- strong fit for scalable event integration architectures

### Production gaps to address

1. Add idempotency and exactly-once processing safeguards where required.
2. Add monitoring, metrics, and observability for Kafka throughput and lag.
3. Add retry policy and backoff strategy definitions.
4. Add schema validation and contract governance for events.
5. Add secure secret handling and environment isolation.
6. Add replay, compaction, and retention policy governance.
7. Add distributed tracing across producers, topics, and consumers.
8. Add DLQ analysis, alerting, and operational dashboards.

## Security considerations

For production deployments, review:

- Kafka authentication and authorization setup
- TLS and certificate handling for cluster access
- service-to-service credential rotation
- retention and privacy controls for event payloads
- sensitive data minimization in event messages
- auditing and replay controls for business-critical domains

## Licensing note

This repository contains a custom commercial license in `LICENSE`.

Important: the license file is the controlling legal document. Before using this code in enterprise or revenue-generating contexts, review the repository license carefully and confirm the allowed usage rights.

## Monetization opportunities

This repo aligns well with several product and service opportunities:

| Business model | Best use case |
| --- | --- |
| event-driven SaaS platform | asynchronous workflow automation |
| integration platform | system-to-system event brokering |
| operational messaging layer | business event processing |
| workflow orchestration service | subscription and event-driven apps |
| enterprise event bus foundation | internal platform modernization |

### Practical paths

- sell event-driven integration services to enterprises
- build distributed workflow layers for business systems
- package the repo as a Kafka event foundation for client projects
- offer modernization of synchronous systems into async event pipelines

## GitHub discoverability

This project is well-positioned around terms such as:

- Kafka event-driven architecture
- Python Kafka producer consumer
- distributed event system
- asynchronous workflow platform
- event sourcing Python project
- CQRS and saga patterns
- enterprise messaging architecture

To improve discoverability:

- emphasize asynchronous system value clearly
- document event contracts and workflow outcomes
- explain operational reliability and retry patterns
- position the repo as a practical distributed systems foundation

## Roadmap ideas

- add richer event schema validation
- support multi-topic orchestration patterns
- add consumer lag and health dashboards
- improve DLQ replay and recovery tooling
- add support for cloud-managed Kafka deployment
- add producer/consumer metrics and alerting
- add exact-once/transactional processing patterns where appropriate

## Contributing

Contributions are welcome for:

- event modeling improvements
- producer/consumer robustness
- retry and DLQ workflow enhancements
- monitoring and operational tooling
- documentation and system onboarding
- production security and deployment patterns

## License

See the repository `LICENSE` file for the full legal terms.
