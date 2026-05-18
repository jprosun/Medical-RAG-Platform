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
