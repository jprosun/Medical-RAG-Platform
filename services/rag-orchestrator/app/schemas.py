from typing import List, Optional, Dict, Any
from pydantic import BaseModel


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    session_id: Optional[str] = None
    message: str


class ChunkDetail(BaseModel):
    id: str
    text: str
    metadata: Dict[str, Any]


class ChatResponse(BaseModel):
    session_id: str
    answer: str
    history: List[ChatMessage]
    context_used: int
    retrieved_chunks: Optional[List[ChunkDetail]] = None
    degraded_mode: bool = False
    degraded_reason: Optional[str] = None
