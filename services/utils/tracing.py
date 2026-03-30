import os
from typing import Optional, Any

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

# Optional instrumentations (only enabled if installed)
try:
    from opentelemetry.instrumentation.requests import RequestsInstrumentor
except Exception:  # pragma: no cover
    RequestsInstrumentor = None  # type: ignore

try:
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
except Exception:  # pragma: no cover
    HTTPXClientInstrumentor = None  # type: ignore

try:
    from opentelemetry.instrumentation.redis import RedisInstrumentor
except Exception:  # pragma: no cover
    RedisInstrumentor = None  # type: ignore

try:
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
except Exception:  # pragma: no cover
    FastAPIInstrumentor = None  # type: ignore


_TRACING_INITIALIZED = False


def setup_tracing(
    app: Optional[Any] = None,
    service_name: Optional[str] = None,
    service_namespace: Optional[str] = "model-serving",
    excluded_urls: str = "/health|/ready|/metrics",
    force: bool = False,
) -> None:
    """
    Shared OpenTelemetry tracing setup for ALL services (FastAPI + Streamlit + jobs).

    - If `app` is provided and FastAPI instrumentation is installed, it instruments routes.
    - Instruments outgoing HTTP via requests/httpx if their instrumentations are installed.
    - Instruments Redis if available.
    - Exports spans via OTLP HTTP exporter.

    Env vars (supported):
      - OTEL_SERVICE_NAME (default service name)
      - OTEL_EXPORTER_OTLP_HTTP_ENDPOINT (default collector endpoint)
      - OTEL_SPAN_PROCESSOR = "batch" | "simple" (default: batch)
    """
    global _TRACING_INITIALIZED

    if _TRACING_INITIALIZED and not force:
        return

    # Allow local/dev runs to disable tracing entirely.
    # Accepted truthy values: 1, true, yes, y, on
    disabled = os.getenv("OTEL_SDK_DISABLED", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }
    if disabled:
        _TRACING_INITIALIZED = True
        return

    # Service name resolution:
    # - prefer explicit argument
    # - else use OTEL_SERVICE_NAME
    # - else fallback
    resolved_service_name = service_name or os.getenv("OTEL_SERVICE_NAME", "streamlit-ui")

    endpoint = os.getenv(
        "OTEL_EXPORTER_OTLP_HTTP_ENDPOINT",
        "http://otel-collector.tracing.svc.cluster.local:4318/v1/traces",
    )
    if not endpoint.strip():
        _TRACING_INITIALIZED = True
        return

    # Resource attributes (keep service.namespace like your orchestrator version)
    resource_attrs = {"service.name": resolved_service_name}
    if service_namespace:
        resource_attrs["service.namespace"] = service_namespace

    provider = TracerProvider(resource=Resource.create(resource_attrs))
    trace.set_tracer_provider(provider)

    exporter = OTLPSpanExporter(endpoint=endpoint)

    span_processor = os.getenv("OTEL_SPAN_PROCESSOR", "batch").strip().lower()
    if span_processor == "simple":
        provider.add_span_processor(SimpleSpanProcessor(exporter))
    else:
        provider.add_span_processor(BatchSpanProcessor(exporter))

    # FastAPI server spans (only if app is given)
    if app is not None and FastAPIInstrumentor is not None:
        FastAPIInstrumentor.instrument_app(app, excluded_urls=excluded_urls)

    # Outbound HTTP instrumentation
    if RequestsInstrumentor is not None:
        RequestsInstrumentor().instrument()

    if HTTPXClientInstrumentor is not None:
        HTTPXClientInstrumentor().instrument()

    # Redis instrumentation (optional)
    if RedisInstrumentor is not None:
        try:
            RedisInstrumentor().instrument()
        except Exception:
            # Don't crash if redis client isn't installed/compatible
            pass

    _TRACING_INITIALIZED = True
