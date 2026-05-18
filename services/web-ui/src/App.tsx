import { useEffect, useState } from "react";

import { checkReadiness, getSession, listSessions, sendChat } from "./api/client";
import { ChatComposer } from "./components/ChatComposer";
import { MessageBubble, type SourceSelection } from "./components/MessageBubble";
import { Sidebar } from "./components/Sidebar";
import { SourceDetailPanel } from "./components/SourceDetailPanel";
import type { AnswerMode, ChatMessage, SessionSummary } from "./types/rag";
import { buildRagSources } from "./utils/sources";

const ACTIVE_SESSION_KEY = "medqa.activeSessionId";

function createSessionId(): string {
  if (globalThis.crypto?.randomUUID) return globalThis.crypto.randomUUID();
  return `web_${Date.now()}_${Math.random().toString(16).slice(2)}`;
}

function latestAssistantSource(messages: ChatMessage[]): SourceSelection | null {
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index];
    if (message.role !== "assistant") continue;
    const source = buildRagSources(message.retrieved_chunks || [])[0];
    if (source) return { source, message };
  }
  return null;
}

function lastAnswerMode(messages: ChatMessage[]): AnswerMode | null {
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const mode = messages[index].metadata?.answer_mode;
    if (mode === "standard" || mode === "thinking") return mode;
  }
  return null;
}

export default function App() {
  const [activeSessionId, setActiveSessionId] = useState<string>(() => {
    return localStorage.getItem(ACTIVE_SESSION_KEY) || createSessionId();
  });
  const [sessions, setSessions] = useState<SessionSummary[]>([]);
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [draft, setDraft] = useState("");
  const [mode, setMode] = useState<AnswerMode>("standard");
  const [selectedSource, setSelectedSource] = useState<SourceSelection | null>(null);
  const [isSending, setIsSending] = useState(false);
  const [isLoadingSession, setIsLoadingSession] = useState(false);
  const [backendReady, setBackendReady] = useState(false);
  const [backendChecked, setBackendChecked] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const refreshSessions = async (silent = false) => {
    try {
      const nextSessions = await listSessions();
      setSessions(nextSessions);
    } catch (err) {
      if (!silent) {
        setError(err instanceof Error ? err.message : "Không tải được danh sách chat.");
      }
    }
  };

  useEffect(() => {
    let cancelled = false;
    let timer: number | undefined;
    let failureCount = 0;

    const probe = async () => {
      const ready = await checkReadiness();
      if (cancelled) return;
      failureCount = ready ? 0 : failureCount + 1;
      setBackendReady(ready);
      setBackendChecked(true);
      if (ready) {
        setError((current) =>
          current === "Backend RAG đang khởi động. Chờ trạng thái Ready rồi gửi lại."
            ? null
            : current,
        );
        void refreshSessions(true);
      }
      const nextDelay = ready ? 15000 : Math.min(30000, 5000 + failureCount * 5000);
      timer = window.setTimeout(probe, nextDelay);
    };

    void probe();
    return () => {
      cancelled = true;
      if (timer) window.clearTimeout(timer);
    };
  }, []);

  useEffect(() => {
    localStorage.setItem(ACTIVE_SESSION_KEY, activeSessionId);
  }, [activeSessionId]);

  const handleNewChat = () => {
    const nextId = createSessionId();
    setActiveSessionId(nextId);
    setMessages([]);
    setSelectedSource(null);
    setDraft("");
    setMode("standard");
    setError(null);
  };

  const handleSelectSession = async (sessionId: string) => {
    if (!sessionId || sessionId === activeSessionId) return;
    setIsLoadingSession(true);
    setError(null);
    try {
      const history = await getSession(sessionId);
      setActiveSessionId(sessionId);
      setMessages(history);
      setSelectedSource(latestAssistantSource(history));
      const historicalMode = lastAnswerMode(history);
      if (historicalMode) setMode(historicalMode);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Không tải được phiên chat.");
    } finally {
      setIsLoadingSession(false);
    }
  };

  const handleSubmit = async () => {
    const question = draft.trim();
    if (!question || isSending) return;
    if (!backendReady) {
      setError("Backend RAG đang khởi động. Chờ trạng thái Ready rồi gửi lại.");
      return;
    }

    const sessionId = activeSessionId || createSessionId();
    setActiveSessionId(sessionId);
    setDraft("");
    setIsSending(true);
    setError(null);

    const now = Date.now() / 1000;
    const pendingId = `pending_${Date.now()}`;
    const userMessage: ChatMessage = {
      role: "user",
      content: question,
      created_at: now,
      _local_id: `user_${Date.now()}`,
    };
    const pendingMessage: ChatMessage = {
      role: "assistant",
      content: "",
      created_at: now,
      metadata: { answer_mode: mode },
      _local_id: pendingId,
      _pending: true,
    };

    setMessages((current) => [...current, userMessage, pendingMessage]);

    try {
      const startedAt = performance.now();
      const response = await sendChat({ sessionId, message: question, answerMode: mode });
      const durationMs = performance.now() - startedAt;
      const nextHistory =
        response.history?.length > 0
          ? response.history
          : [
              ...messages,
              userMessage,
              {
                role: "assistant" as const,
                content: response.answer,
                context_used: response.context_used,
                retrieved_chunks: response.retrieved_chunks || [],
                external_sources: response.external_sources || [],
                metadata: response.metadata || {},
                duration_ms: durationMs,
                degraded_mode: response.degraded_mode,
                degraded_reason: response.degraded_reason,
              },
            ];

      setMessages(nextHistory);
      setSelectedSource(latestAssistantSource(nextHistory));
      await refreshSessions();
    } catch (err) {
      const message = err instanceof Error ? err.message : "Gửi câu hỏi thất bại.";
      setError(message);
      setMessages((current) =>
        current.map((item) =>
          item._local_id === pendingId
            ? {
                ...item,
                content: `Không thể tạo câu trả lời.\n\n${message}`,
                _pending: false,
                _error: true,
              }
            : item,
        ),
      );
    } finally {
      setIsSending(false);
    }
  };

  return (
    <div className="appShell">
      <Sidebar
        activeSessionId={activeSessionId}
        onNewChat={handleNewChat}
        onSelectSession={handleSelectSession}
        sessions={sessions}
      />

      <main className="chatArea">
        <header className="topBar">
          <div>
            <span className="eyebrow">RAG chatbot y khoa</span>
            <h1>Hỏi đáp y khoa có evidence</h1>
          </div>
          <div className="topStatus">
            <span>{mode === "thinking" ? "Thinking mode" : "Standard mode"}</span>
            <small>{activeSessionId.slice(0, 8)}</small>
          </div>
        </header>

        {!backendReady && (
          <div className="warningBanner">
            {backendChecked
              ? "Backend RAG chưa ready. Docker đang preload embedding model, thường mất 2-3 phút sau khi start."
              : "Đang kiểm tra backend RAG..."}
          </div>
        )}
        {error && <div className="errorBanner">{error}</div>}

        <section className="messagePane">
          {isLoadingSession ? (
            <div className="centerState">Đang tải lại đoạn chat...</div>
          ) : messages.length === 0 ? (
            <div className="welcomeCard">
              <span>MedQA Assistant</span>
              <h2>Đặt câu hỏi y khoa, hệ thống sẽ retrieval và ghi citation rõ.</h2>
              <p>
                Standard ưu tiên tốc độ và đủ ý. Thinking đào sâu hơn, dùng nhiều retrieval
                hơn và vẫn tuân thủ rule citation.
              </p>
            </div>
          ) : (
            messages.map((message, index) => (
              <MessageBubble
                key={message._local_id || `${message.role}-${index}-${message.created_at || index}`}
                message={message}
                onSelectSource={setSelectedSource}
                selectedSourceKey={selectedSource?.source.key}
              />
            ))
          )}
        </section>

        <div className="composerDock">
          <ChatComposer
            disabled={isSending || !backendReady}
            mode={mode}
            onChange={setDraft}
            onModeChange={setMode}
            onSubmit={handleSubmit}
            value={draft}
          />
        </div>
      </main>

      <SourceDetailPanel selection={selectedSource} />
    </div>
  );
}
