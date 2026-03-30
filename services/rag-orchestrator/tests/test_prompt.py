from app.prompt import build_prompt
from app.retriever import RetrievedChunk


def test_build_prompt_with_context():
    chunks = [
        RetrievedChunk(id="1", text="Heart rate is measured in bpm.", score=0.9, metadata={}),
        RetrievedChunk(id="2", text="Normal adult heart rate is 60–100 bpm.", score=0.8, metadata={}),
    ]

    prompt = build_prompt("What is heart rate?", chunks)

    assert "QUESTION:" in prompt
    assert "CONTEXT:" in prompt
    assert "[source:1]" in prompt
    assert "[source:2]" in prompt
    assert "Heart rate is measured" in prompt


def test_build_prompt_no_context():
    prompt = build_prompt("What is heart rate?", [])

    assert "NO_CONTEXT" in prompt
    assert "What is heart rate?" in prompt

def test_build_prompt_with_history():
    history = [
        {"role": "user", "content": "Hi"},
        {"role": "assistant", "content": "Hello"},
    ]

    prompt = build_prompt(
        "What is blood pressure?",
        [],
        chat_history=history,
    )

    assert "CHAT_HISTORY" in prompt
    assert "USER: Hi" in prompt
    assert "ASSISTANT: Hello" in prompt

def test_build_prompt_none_history_equals_empty():
    p1 = build_prompt("Q", [], chat_history=None)
    p2 = build_prompt("Q", [], chat_history=[])

    assert p1 == p2