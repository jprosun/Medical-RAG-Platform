from app.schemas import ChatRequest, ChatResponse, ChatMessage
from pydantic import ValidationError


def test_chat_request_valid():
    req = ChatRequest(message="hello")
    assert req.message == "hello"
    assert req.session_id is None


def test_chat_request_invalid():
    try:
        ChatRequest()
        assert False, "Expected ValidationError"
    except ValidationError:
        pass


def test_chat_response():
    resp = ChatResponse(
        session_id="abc",
        answer="test",
        history=[ChatMessage(role="user", content="hi")],
        context_used=2,
    )
    assert resp.context_used == 2
