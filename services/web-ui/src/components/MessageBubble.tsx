import type { ChatMessage, RagSource } from "../types/rag";
import { buildRagSources, citationTokenForSource, shorten, shortId } from "../utils/sources";

export type SourceSelection = {
  source: RagSource;
  message: ChatMessage;
};

type MessageBubbleProps = {
  message: ChatMessage;
  onSelectSource: (selection: SourceSelection) => void;
  selectedSourceKey?: string;
};

function metricLabel(message: ChatMessage): string {
  const metadata = message.metadata || {};
  const chunks = message.retrieved_chunks?.length ?? message.context_used ?? 0;
  const totalMs = metadata.timings_ms?.total_ms || message.duration_ms;
  const seconds = typeof totalMs === "number" ? `${(totalMs / 1000).toFixed(1)}s` : "";
  const parts = [
    metadata.answer_mode ? `Mode: ${metadata.answer_mode}` : "",
    metadata.answer_policy ? `Policy: ${metadata.answer_policy}` : "",
    metadata.coverage_mode || metadata.coverage_level
      ? `Coverage: ${metadata.coverage_mode || metadata.coverage_level}`
      : "",
    chunks ? `Chunks: ${chunks}` : "",
    seconds,
  ].filter(Boolean);
  return parts.join("  ");
}

function renderInline(
  text: string,
  sources: RagSource[],
  message: ChatMessage,
  onSelectSource: (selection: SourceSelection) => void,
) {
  const parts = text.split(/(\[(?:E)?\d+\])/g);
  return parts.map((part, index) => {
    const match = part.match(/^\[(\d+)\]$/);
    if (!match) {
      return <span key={`${part}-${index}`}>{part}</span>;
    }
    const source = sources.find((item) => item.number === Number(match[1]));
    if (!source) {
      return <span key={`${part}-${index}`}>{part}</span>;
    }
    return (
      <button
        className="citationButton"
        key={`${source.key}-${index}`}
        onClick={() => onSelectSource({ source, message })}
        title={source.title}
        type="button"
      >
        {part}
      </button>
    );
  });
}

function AnswerText({
  content,
  sources,
  message,
  onSelectSource,
}: {
  content: string;
  sources: RagSource[];
  message: ChatMessage;
  onSelectSource: (selection: SourceSelection) => void;
}) {
  const lines = content.split(/\r?\n/);

  return (
    <div className="answerText">
      {lines.map((line, index) => {
        const trimmed = line.trim();
        if (!trimmed) {
          return <div className="answerSpacer" key={`spacer-${index}`} />;
        }
        if (/^#{1,3}\s+/.test(trimmed)) {
          return (
            <h3 key={`heading-${index}`}>
              {renderInline(trimmed.replace(/^#{1,3}\s+/, ""), sources, message, onSelectSource)}
            </h3>
          );
        }
        if (/^[-*]\s+/.test(trimmed)) {
          return (
            <p className="answerBullet" key={`bullet-${index}`}>
              {renderInline(trimmed.replace(/^[-*]\s+/, ""), sources, message, onSelectSource)}
            </p>
          );
        }
        if (/^\d+\.\s+/.test(trimmed)) {
          return (
            <p className="answerBullet" key={`number-${index}`}>
              {renderInline(trimmed, sources, message, onSelectSource)}
            </p>
          );
        }
        return (
          <p key={`line-${index}`}>
            {renderInline(trimmed, sources, message, onSelectSource)}
          </p>
        );
      })}
    </div>
  );
}

export function MessageBubble({
  message,
  onSelectSource,
  selectedSourceKey,
}: MessageBubbleProps) {
  const isAssistant = message.role === "assistant";
  const sources = isAssistant ? buildRagSources(message.retrieved_chunks || []) : [];

  return (
    <article className={`messageRow ${message.role}`}>
      <div className="messageAvatar">{message.role === "assistant" ? "AI" : "Bạn"}</div>
      <div className={`messageCard ${message._pending ? "pending" : ""} ${message._error ? "error" : ""}`}>
        {isAssistant && <div className="messageMeta">{metricLabel(message)}</div>}
        {isAssistant ? (
          <AnswerText
            content={message.content}
            message={message}
            onSelectSource={onSelectSource}
            sources={sources}
          />
        ) : (
          <div className="userText">{message.content}</div>
        )}

        {message._pending && (
          <div className="thinkingLoader">
            <span />
            <span />
            <span />
            Đang retrieval, rerank và viết câu trả lời...
          </div>
        )}

        {sources.length > 0 && (
          <section className="sourceStrip" aria-label="Tài liệu truy hồi">
            <div className="sourceStripHeader">
              <span>Tài liệu truy hồi</span>
              <small>{sources.length} nguồn đã gom theo article/title</small>
            </div>
            <div className="sourceCards">
              {sources.map((source) => (
                <button
                  className={`sourceCard ${source.key === selectedSourceKey ? "active" : ""}`}
                  key={source.key}
                  onClick={() => onSelectSource({ source, message })}
                  type="button"
                >
                  <div className="sourceTopLine">
                    <strong>{citationTokenForSource(source)}</strong>
                    <span>{source.sourceName}</span>
                    {source.score != null && <em>{source.score.toFixed(3)}</em>}
                  </div>
                  <div className="sourceTitle">{source.title}</div>
                  <div className="sourceSnippet">
                    {shorten(source.snippets[0]?.text || "", 210)}
                  </div>
                  <div className="sourceFoot">
                    <span>ID: {shortId(source.articleId || source.docId)}</span>
                    {source.sectionTitle && <span>{source.sectionTitle}</span>}
                  </div>
                </button>
              ))}
            </div>
          </section>
        )}
      </div>
    </article>
  );
}
