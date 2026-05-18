import type { SessionSummary } from "../types/rag";

type SidebarProps = {
  activeSessionId: string;
  sessions: SessionSummary[];
  onNewChat: () => void;
  onSelectSession: (sessionId: string) => void;
};

function formatSessionTime(value?: number): string {
  if (!value) return "";
  const date = new Date(value * 1000);
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleString("vi-VN", {
    day: "2-digit",
    month: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

export function Sidebar({
  activeSessionId,
  sessions,
  onNewChat,
  onSelectSession,
}: SidebarProps) {
  return (
    <aside className="sidebar">
      <div className="brandBlock">
        <div className="brandMark">M</div>
        <div>
          <div className="brandTitle">MedQA RAG</div>
          <div className="brandSub">Professional medical explainer</div>
        </div>
      </div>

      <button className="newChatButton" onClick={onNewChat} type="button">
        Tạo đoạn chat mới
      </button>

      <div className="sidebarSectionTitle">Đoạn chat gần đây</div>
      <div className="sessionList">
        {sessions.length === 0 ? (
          <div className="emptySessions">Chưa có phiên chat đã lưu.</div>
        ) : (
          sessions.map((session) => (
            <button
              className={`sessionItem ${session.id === activeSessionId ? "active" : ""}`}
              key={session.id}
              onClick={() => onSelectSession(session.id)}
              type="button"
            >
              <span>{session.title || "Đoạn chat mới"}</span>
              <small>{formatSessionTime(session.updated_at)}</small>
            </button>
          ))
        )}
      </div>
    </aside>
  );
}
