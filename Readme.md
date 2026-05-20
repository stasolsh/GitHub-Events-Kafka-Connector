# GitHub Events Kafka Connector

[![Java](https://img.shields.io/badge/Java-17-orange.svg)](https://openjdk.org/)
[![Gradle](https://img.shields.io/badge/Gradle-8.x-02303A.svg?logo=gradle)](https://gradle.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.9-black.svg?logo=apachekafka)](https://kafka.apache.org/)
[![Kafka Connect](https://img.shields.io/badge/Kafka%20Connect-Source%20Connector-blue.svg)](https://kafka.apache.org/documentation/#connect)
[![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED.svg?logo=docker)](https://www.docker.com/)
[![GitHub Actions](https://img.shields.io/github/actions/workflow/status/stasolsh/github-events-kafka-connector/ci.yml?branch=main)](https://github.com/stasolsh/github-events-kafka-connector/actions)
[![Codecov](https://img.shields.io/codecov/c/github/stasolsh/github-events-kafka-connector)](https://codecov.io/gh/stasolsh/github-events-kafka-connector)
[![License](https://img.shields.io/github/license/stasolsh/github-events-kafka-connector)](LICENSE)
[![Issues](https://img.shields.io/github/issues/stasolsh/github-events-kafka-connector)](https://github.com/stasolsh/github-events-kafka-connector/issues)
[![Stars](https://img.shields.io/github/stars/stasolsh/github-events-kafka-connector)](https://github.com/stasolsh/github-events-kafka-connector/stargazers)
[![Forks](https://img.shields.io/github/forks/stasolsh/github-events-kafka-connector)](https://github.com/stasolsh/github-events-kafka-connector/network)
[![Last Commit](https://img.shields.io/github/last-commit/stasolsh/github-events-kafka-connector)](https://github.com/stasolsh/github-events-kafka-connector/commits/main)
[![Open PRs](https://img.shields.io/github/issues-pr/stasolsh/github-events-kafka-connector)](https://github.com/stasolsh/github-events-kafka-connector/pulls)
[![Maintenance](https://img.shields.io/badge/Maintained-yes-green.svg)](https://github.com/stasolsh/github-events-kafka-connector)
[![Contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg)](CONTRIBUTING.md)

---

## Overview

Custom Kafka Connect Source Connector that continuously polls GitHub repository events and publishes them into Apache Kafka topics.

This project demonstrates:

- Kafka Connect Source Connector development
- GitHub REST API integration
- Kafka event streaming
- Dockerized infrastructure
- DTO-based JSON mapping
- Offset management
- CI/CD with GitHub Actions
- Code coverage reporting with Codecov

---

## Architecture

```text
GitHub Repository Events
            ↓
     GitHub REST API
            ↓
 GitHub Source Connector
            ↓
      Kafka Connect
            ↓
       Apache Kafka
            ↓
    Kafka Consumers
```

---

## Tech Stack

| Technology | Purpose |
|---|---|
| Java 17 | Main language |
| Apache Kafka | Event streaming |
| Kafka Connect | Connector framework |
| Docker | Containerization |
| Gradle | Build tool |
| Jackson | JSON serialization/deserialization |
| JUnit 5 | Testing |
| Mockito | Mocking |
| Testcontainers | Integration testing |
| GitHub Actions | CI/CD |
| Codecov | Test coverage |

---

## Features

- Poll GitHub repository events
- Publish events to Kafka topics
- Store Kafka Connect offsets
- Support authenticated GitHub API access
- DTO-based API mapping
- Docker Compose environment
- Unit & integration tests
- CI/CD pipeline
- Coverage reporting

---

## Project Structure

```text
src
├── main
│   ├── java
│   │   └── com.example.github.connector
│   │       ├── dto
│   │       ├── GithubApiClient
│   │       ├── GithubEventsSourceConnector
│   │       ├── GithubEventsSourceTask
│   │       └── GithubEventsConnectorConfig
│   └── resources
│       └── META-INF/services
│
├── test
│   └── java
│       └── com.example.github.connector
```

---

## Running Locally

### Clone Repository

```bash
git clone https://github.com/stasolsh/github-events-kafka-connector.git
cd github-events-kafka-connector
```

---

### Build Project

```bash
./gradlew clean shadowJar
```

Windows:

```powershell
.\gradlew.bat clean shadowJar
```

---

### Start Infrastructure

```bash
docker compose up -d
```

---

### Verify Kafka Connect

```bash
curl http://localhost:8083/connector-plugins
```

Expected:

```json
[
  {
    "class": "com.example.github.connector.GithubEventsSourceConnector",
    "type": "source",
    "version": "1.0.0"
  }
]
```

---

## Register Connector

```bash
curl -X POST http://localhost:8083/connectors \
-H "Content-Type: application/json" \
-d @connector/github-source-connector.json
```

---

## Consume Events

```bash
docker exec -it kafka kafka-console-consumer \
--bootstrap-server localhost:9092 \
--topic github.events \
--from-beginning
```

---

## Example Event

```json
{
  "github_event_id": "123456789",
  "type": "PushEvent",
  "repo": "apache/kafka",
  "actor": "developer",
  "created_at": "2026-05-20T10:00:00Z",
  "payload": {
    "ref": "refs/heads/main"
  }
}
```

---

## Running Tests

```bash
./gradlew test
```

Generate coverage report:

```bash
./gradlew jacocoTestReport
```

Coverage report:

```text
build/reports/jacoco/test/html/index.html
```

---

## CI/CD

GitHub Actions pipeline includes:

- Build
- Unit tests
- Integration tests
- JaCoCo coverage
- Codecov upload

---