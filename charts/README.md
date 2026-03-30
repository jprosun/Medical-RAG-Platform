# About Helm chart designs

This document describes the **Kubernetes pod architecture**, **workload types**, and **components communication** used in the LLM-MedQA-Assistant platform.

The system is designed following **namespace separation**, **stateless vs stateful workload patterns**, and **observability**.

---

## 1. Namespace Responsibilities

| Namespace        | Responsibility |
|------------------|----------------|
| `ingress-nginx`  | Traffic entry into the cluster |
| `model-serving`  | Core application logic, RAG pipeline, vector DB, sessions |
| `logging`        | Centralized log collection, parsing, storage, and visualization |
| `monitoring`     | Metrics scraping, storage, and dashboards |

---

## 2. Ingress Namespace

### Purpose
Handles **external HTTP/HTTPS traffic** and routes it to internal services using Kubernetes Ingress resources.

### Pods

| Pod | Pod Type | What it does | Why this type | Communicates with | How it communicates |
|----|---------|--------------|--------------|------------------|---------------------|
| ingress-nginx-controller | Deployment | Acts as Kubernetes Ingress Controller | Stateless controller pattern defined by ingress-nginx Helm chart | Streamlit service (`model-serving`) | HTTP/HTTPS via Ingress rules -> ClusterIP Service |

**Why Deployment?**  
Ingress controllers do not store state and can be horizontally scaled for availability.

---

## 3. Logging Namespace (ELK Stack)

### Purpose
Provides **cluster-wide log aggregation** using a classic **ELK + Beats** pipeline.

### Pods

| Pod | Pod Type | What it does | Why this type | Communicates with | How it communicates |
|----|---------|--------------|--------------|------------------|---------------------|
| elasticsearch-0 | StatefulSet | Stores logs and indices | Requires stable identity & persistent storage (PVC) | Logstash, Kibana | TCP 9200 |
| logstash-* | Deployment | Parses and forwards logs | Stateless pipeline workers | Elasticsearch | TCP 9200 |
| kibana-* | Deployment | Visualizes logs | Stateless UI | Elasticsearch | HTTP 9200 |
| filebeat-* | DaemonSet | Collects node & pod logs | Must run on every node | Logstash | TCP 5044 |

**Why DaemonSet for Filebeat?**  
Log agents must run **1 pod per node** to access `/var/log` and container logs reliably.

---

## 4. Model-Serving Namespace

### Purpose
Hosts the **RAG pipeline**, **vector database**, **session storage**, and **UI**.

### Data & Storage Components

| Pod | Pod Type | What it does | Why this type | Communicates with | How it communicates |
|----|---------|--------------|--------------|------------------|---------------------|
| qdrant-0 | StatefulSet | Stores vector embeddings | Persistent DB with PVC | RAG Orchestrator | HTTP (6333) via ClusterIP |
| redis-0 | StatefulSet | Session storage | Stateful in-memory DB | RAG Orchestrator | TCP 6379 |

### Initialization Jobs

| Pod | Pod Type | What it does | Why this type | Communicates with | How it communicates |
|----|---------|--------------|--------------|------------------|---------------------|
| qdrant-init-* | Job | Initializes Qdrant collections | One-time bootstrap task | Qdrant | HTTP bootstrap calls |

### Application Layer

| Pod | Pod Type | What it does | Why this type | Communicates with | How it communicates |
|----|---------|--------------|--------------|------------------|---------------------|
| rag-orchestrator-* | Deployment | Handles RAG pipeline logic | Stateless API service | Redis, Qdrant, external LLM | HTTP + Redis TCP |
| streamlit-* | Deployment | User interface | Stateless frontend | RAG Orchestrator | HTTP via Service |

**Key Design Principle:**  
Only **datastores** are StatefulSets.  
All **business logic and UI** are Deployments.

---

## 5. Monitoring Namespace

### Purpose
Provides **metrics collection and visualization** for workloads and infrastructure.

### Pods

| Pod | Pod Type | What it does | Why this type | Communicates with | How it communicates |
|----|---------|--------------|--------------|------------------|---------------------|
| prometheus-* | Deployment | Scrapes & stores metrics | Config-driven, no PVC | All metrics endpoints | HTTP scrape |
| grafana-* | Deployment | Metrics visualization | Stateless UI | Prometheus | HTTP queries |

---
## 6. Tracing Namespace (Distributed Tracing)
### Purpose
The `tracing` namespace provides **distributed tracing** for the LLM-MedQA-Assistant platform. It enables end-to-end visibility of a single user request as it flows from the **Streamlit UI**, through the **RAG Orchestrator**, and (optionally in the future) to the **external inference service**.

### Pods
### Tracing Namespace Pods

| Pod | Pod Type | What it does | Why this type | Communicates with | How it communicates |
|---|---|---|---|---|---|
| `otel-collector-*` | Deployment | Receives, processes, and exports distributed traces | Stateless component that can be horizontally scaled and restarted safely | Streamlit UI, RAG Orchestrator, Jaeger | OTLP over HTTP (4318) and gRPC (4317) |
| `jaeger-*` | Deployment | Stores and visualizes trace data; provides Jaeger UI | Stateless query and UI service with no application-owned persistent state | OpenTelemetry Collector, users (via browser) | OTLP gRPC from collector; HTTP (16686) for UI |
---

## 7. Service Topology

| Component | Namespace | Service Name | Service Type | Purpose |
|---------|----------|--------------|--------------|--------|
| Streamlit UI | model-serving | streamlit | ClusterIP | Internal UI exposed via Ingress |
| RAG Orchestrator | model-serving | rag-orchestrator | ClusterIP | Internal API |
| Qdrant | model-serving | qdrant | ClusterIP | Vector DB access |
| Redis | model-serving | redis | ClusterIP | Session store |
| Elasticsearch | logging | elasticsearch | ClusterIP | Log storage |
| Logstash | logging | logstash | ClusterIP | Log ingestion |
| Kibana | logging | kibana | ClusterIP | Log visualization |
| Prometheus | monitoring | prometheus | ClusterIP | Metrics backend |
| Grafana | monitoring | grafana | ClusterIP | Metrics UI |
| OpenTelemetry Collector | tracing | otel-collector | ClusterIP | Receives and forwards to the tracing backend |
| Jaeger | tracing | jaeger | ClusterIP | Provides Jaeger Web UI |

**Design Choice:**  
All services are **ClusterIP**.  
External exposure is handled **only by Ingress**, not by LoadBalancers per service.

---

## 8. Workload Type Comparison

| Criteria | Deployment | StatefulSet | DaemonSet |
|--------|------------|------------|-----------|
| Has state | X | O | X |
| Stable pod name | X | O | X |
| Uses PVC | X | O | X |
| Scales replicas | X | O | X |
| One pod per node | X | X | O |
| Typical usage | API / UI | DB / Storage | Agents |

---

## 9. Architectural Summary

- **Clear separation of concerns by namespace**
- **Ingress-only external exposure**
- **Stateful components isolated and minimal**
- **Observability treated as first-class (logs + metrics)**

This layout reflects **Kubernetes design** and supports scalability, observability, and maintainability.

---

# About security design
Security is the pratice of protecting information and systems from unauthorized access, use, disclosure, disruption, modification,
or destruction.  
It is traditionally defined by the three core principles of the CIA Triad:
- Confidentially: Keep data secret (only authorized people can see it)
- Integrity: Keep data accurate (prevent unauthorized changes)
- Availability: Keep the system running (ensuring the service is there when needed)
---
## LLMOps Life Cycle – Security Considerations

| Plan / Phase | Security Considerations | Tools to Prevent / Mitigate |
|-------------|------------------------|-----------------------------|
| **Phase 1 – Plan / Scope** | - Threat modeling (prompt injection, data leakage)<br>- Data privacy & compliance (PII handling, legal compliance)<br>- Third-party model risk (model safety, provider trustworthiness)<br>- Access & governance (who can query the model, authority levels) | - OWASP Top 10 for LLMs<br>- MITRE ATLAS<br>- Microsoft Presidio<br>- picklescan, ModelScan<br>- SOC2 / ISO 27001 reports (providers)<br>- RBAC (AWS IAM, Azure Active Directory, Kubernetes RBAC) |
| **Phase 2 – Data Augmentation & Fine-Tuning** | - Adversarial robustness testing<br>- Vulnerability assessment of ML libraries<br>- License compliance scanning<br>- Boundary protection (toxic content, sensitive data leakage)<br>- Data integrity & validation (poisoned datasets, PII prevention)<br>- Pipeline & storage security (ETL and fine-tuning access control) | - ART, NVIDIA Garak<br>- safety, Snyk, pip-audit<br>- FOSSA<br>- NeMo Guardrails, Guardrails AI<br>- Great Expectations, Deequ<br>- DataHub<br>- Apache Airflow with RBAC |
| **Phase 3 – Application Development** | - Application security testing (code & runtime)<br>- Hardcoded secrets, weak cryptography, unsafe functions<br>- Runtime vulnerability detection<br>- Secure developer access & MFA<br>- Secure LLM–application interaction (API tokens, keys) | - SAST, DAST, IAST<br>- Snyk, SonarQube<br>- GitLab, Auth0, AWS IAM<br>- HashiCorp Vault |
| **Phase 4 – Release** | - Infrastructure-related security<br>- Service-to-service authentication & least privilege<br>- Network security (private VPC, mTLS)<br>- Trusted execution environments<br>- Infrastructure-as-Code (IaC) scanning<br>- Model & dataset integrity (signing)<br>- CI/CD pipeline hardening | - IAM (Workload Identity, MFA)<br>- VPC, mTLS<br>- Checkov, Terrascan<br>- sigstore<br>- GitLab CI/CD<br>- HashiCorp Vault<br>- Auth0, AWS IAM |
| **Phase 5 – Operate & Monitoring** | - Real-time guardrails for inputs & outputs<br>- Prompt security (system prompt leakage)<br>- Patch management for dependencies<br>- Real-time privacy protection (PII detection & masking) | - NVIDIA NeMo Guardrails<br>- Guardrails AI<br>- Promptfoo<br>- Dependabot<br>- Microsoft Presidio |

---

## LLMOps Life Cycle – Alignment with LLM-MedQA / RAG Kubernetes Architecture

| Plan / Phase | Security Considerations | Tool Choice | What It Can Prevent (At Least) |
|-------------|----------------------------------|----------------------------------------|--------------------------------|
| **Phase 1 – Plan / Scope** | - Prompt injection<br>- Data leakage (PII exposure)<br>- Untrusted third-party models<br>- Over-permissive access to services | **OWASP Top 10 for LLMs (Checklist)** | - Prevents missing obvious LLM threats early<br>- Helps avoid insecure design decisions (e.g. no prompt boundaries, no access rules) |
| **Phase 2 – Data Augmentation & Fine-Tuning** | - Poisoned datasets<br>- Accidental PII in training data<br>- Corrupted or low-quality data entering Qdrant | **NVIDIA NeMo Guardrails** | - Prevent conversations off-topic<br>- Prevent adversarial attempts from bypassing safety measures and manipulating the LLM into generating harmful or unwanted content |
| **Phase 3 – Application Development** | - Hardcoded secrets<br>- Insecure code patterns<br>- Vulnerable dependencies<br>- Unsafe Python functions | **SonarQube** | - Flags obvious insecure code and dependencies |
| **Phase 4 – Release** | - Misconfigured cloud resources<br>- Public buckets or open ports<br>- Over-privileged service accounts | **Checkov** | - Prevents common Terraform / IaC misconfigurations<br>- Catches public-exposed resources before deployment |
| **Phase 5 – Operate & Monitoring** | - Prompt leakage (system prompt exposed)<br>- Toxic or unsafe outputs<br>- Runtime data leakage<br>- Undetected abnormal behavior | **Prometheus + Grafana** | - Prevents silent failures by exposing abnormal traffic, latency, or error rates<br>- Helps detect misuse or attacks indirectly through metrics |
