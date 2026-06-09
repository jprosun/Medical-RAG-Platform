import type { AnswerMode, ChatResponse, SessionSummary } from "../types/rag";

const API_BASE = import.meta.env.VITE_RAG_API_URL || "/api";
const HEALTH_BASE =
  import.meta.env.VITE_RAG_HEALTH_URL ||
  (API_BASE === "/api" ? "http://localhost:8000" : API_BASE.replace(/\/api\/?$/, ""));

async function requestJson<T>(path: string, init?: RequestInit): Promise<T> {
  let response: Response;
  try {
    response = await fetch(`${API_BASE}${path}`, {
      headers: {
        "Content-Type": "application/json",
        ...(init?.headers || {}),
      },
      ...init,
    });
  } catch (error) {
    throw new Error(
      error instanceof Error
        ? `Không kết nối được backend RAG: ${error.message}`
        : "Không kết nối được backend RAG.",
    );
  }
  if (!response.ok) {
    const text = await response.text().catch(() => "");
    let message = text || `Request failed: ${response.status}`;
    try {
      const parsed = JSON.parse(text) as { detail?: unknown };
      if (typeof parsed.detail === "string") message = parsed.detail;
    } catch {
      // Keep raw text for non-JSON errors, for example Vite proxy 500.
    }
    if (response.status >= 500 && message.includes("Request failed")) {
      message = "Backend RAG đang khởi động hoặc proxy chưa kết nối. Chờ backend ready rồi gửi lại.";
    }
    throw new Error(message);
  }
  return response.json() as Promise<T>;
}

export async function checkReadiness(timeoutMs = 2500): Promise<boolean> {
  const controller = new AbortController();
  const timeout = window.setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(`${HEALTH_BASE}/ready`, {
      cache: "no-store",
      signal: controller.signal,
    });
    return response.ok;
  } catch {
    return false;
  } finally {
    window.clearTimeout(timeout);
  }
}

export async function listSessions(): Promise<SessionSummary[]> {
  const data = await requestJson<{ sessions: SessionSummary[] }>("/sessions");
  return [...(data.sessions || [])].sort((a, b) => (b.updated_at || 0) - (a.updated_at || 0));
}

export async function getSession(sessionId: string): Promise<ChatResponse["history"]> {
  const data = await requestJson<{ messages: ChatResponse["history"] }>(`/session/${encodeURIComponent(sessionId)}`);
  return data.messages || [];
}

export async function deleteSession(sessionId: string): Promise<void> {
  await requestJson(`/session/${encodeURIComponent(sessionId)}`, { method: "DELETE" });
}

export async function renameSession(sessionId: string, title: string): Promise<void> {
  await requestJson(`/session/${encodeURIComponent(sessionId)}/title`, {
    method: "PUT",
    body: JSON.stringify({ title }),
  });
}

export async function sendChat(params: {
  sessionId: string;
  message: string;
  answerMode: AnswerMode;
}): Promise<ChatResponse> {
  return requestJson<ChatResponse>("/chat", {
    method: "POST",
    body: JSON.stringify({
      session_id: params.sessionId,
      message: params.message,
      answer_mode: params.answerMode,
    }),
  });
}

export type StreamEvent =
  | { type: "stage"; stage: string; label: string }
  | { type: "token"; delta: string }
  | { type: "final" } & ChatResponse
  | { type: "error"; detail: string };

/**
 * Stream a chat answer over SSE. Calls onEvent for every server event
 * (stage progress, answer token, final payload). Throws before the first
 * event if the stream cannot be opened, so the caller can fall back to
 * the non-streaming endpoint.
 */
export async function sendChatStream(
  params: { sessionId: string; message: string; answerMode: AnswerMode },
  onEvent: (event: StreamEvent) => void,
  signal?: AbortSignal,
): Promise<void> {
  const response = await fetch(`${API_BASE}/chat/stream`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Accept: "text/event-stream" },
    body: JSON.stringify({
      session_id: params.sessionId,
      message: params.message,
      answer_mode: params.answerMode,
    }),
    signal,
  });

  if (!response.ok || !response.body) {
    throw new Error(`Stream failed: ${response.status}`);
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  const flush = (chunk: string) => {
    buffer += chunk;
    let boundary = buffer.indexOf("\n\n");
    while (boundary !== -1) {
      const raw = buffer.slice(0, boundary);
      buffer = buffer.slice(boundary + 2);
      const dataLine = raw
        .split("\n")
        .find((line) => line.startsWith("data:"));
      if (dataLine) {
        const json = dataLine.slice(5).trim();
        if (json) {
          try {
            onEvent(JSON.parse(json) as StreamEvent);
          } catch {
            // Ignore malformed SSE frames; keep reading the stream.
          }
        }
      }
      boundary = buffer.indexOf("\n\n");
    }
  };

  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    flush(decoder.decode(value, { stream: true }));
  }
  flush(decoder.decode());
}
