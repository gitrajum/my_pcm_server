"""
Telemetry Service (Optional)
Provides OpenTelemetry instrumentation for the MCP server.
"""

import logging

try:
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import SERVICE_NAME, Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
except ImportError:
    raise ImportError(
        "Telemetry requires OpenTelemetry packages. "
        "Install with: pip install elsa-mcp[telemetry]"
    )

logger = logging.getLogger(__name__)


def setup_telemetry(settings):
    """
    Setup OpenTelemetry instrumentation.

    Args:
        settings: Application settings with telemetry configuration
    """
    # Create resource
    resource = Resource(attributes={SERVICE_NAME: settings.otel_service_name})

    # Create tracer provider
    tracer_provider = TracerProvider(resource=resource)

    # Create OTLP exporter
    otlp_exporter = OTLPSpanExporter(endpoint=f"{settings.otel_exporter_otlp_endpoint}/v1/traces")

    # Add span processor
    span_processor = BatchSpanProcessor(otlp_exporter)
    tracer_provider.add_span_processor(span_processor)

    # Set global tracer provider
    trace.set_tracer_provider(tracer_provider)

    logger.info(
        f"OpenTelemetry configured: service={settings.otel_service_name}, "
        f"endpoint={settings.otel_exporter_otlp_endpoint}"
    )


def get_tracer(name: str):
    """
    Get a tracer instance.

    Args:
        name: Name for the tracer

    Returns:
        Tracer instance
    """
    return trace.get_tracer(name)
