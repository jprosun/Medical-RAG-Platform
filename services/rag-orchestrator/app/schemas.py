from typing import List, Optional, Dict, Any
from pydantic import BaseModel


class ChatMessage(BaseModel):
    role: str
    content: str
    created_at: Optional[float] = None
    context_used: Optional[int] = None
    retrieved_chunks: Optional[List[Dict[str, Any]]] = None
    metadata: Optional[Dict[str, Any]] = None
    external_sources: Optional[List[Dict[str, Any]]] = None
    duration_ms: Optional[float] = None
    degraded_mode: Optional[bool] = None
    degraded_reason: Optional[str] = None


class ChatRequest(BaseModel):
    session_id: Optional[str] = None
    message: str
    answer_mode: Optional[str] = None


class ChunkDetail(BaseModel):
    id: str
    text: str
    score: Optional[float] = None
    metadata: Dict[str, Any]


class ChatResponse(BaseModel):
    session_id: str
    answer: str
    history: List[ChatMessage]
    context_used: int
    retrieved_chunks: Optional[List[ChunkDetail]] = None
    metadata: Dict[str, Any] = {}
    external_sources: List[Dict[str, Any]] = []
    degraded_mode: bool = False
    degraded_reason: Optional[str] = None
