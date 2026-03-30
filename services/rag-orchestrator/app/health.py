import os
import socket
from typing import Dict
from http.server import BaseHTTPRequestHandler, HTTPServer

def tcp_check(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False

def readiness() -> Dict[str, bool]:
    checks = {}

    # Optional deps — only check if env vars exist
    if os.getenv("REDIS_HOST"):
        checks["redis"] = tcp_check(
            os.getenv("REDIS_HOST"),
            int(os.getenv("REDIS_PORT", "6379"))
        )

    if os.getenv("QDRANT_HOST"):
        checks["qdrant"] = tcp_check(
            os.getenv("QDRANT_HOST"),
            int(os.getenv("QDRANT_PORT", "6333"))
        )

    if os.getenv("KSERVE_HOST"):
        checks["kserve"] = tcp_check(
            os.getenv("KSERVE_HOST"),
            int(os.getenv("KSERVE_PORT", "80"))
        )

    # If no optional deps configured, we are ready
    return checks

def liveness() -> Dict[str, str]:
    """
    Only check that the process is alive :>
    """
    return {"status": "alive"}