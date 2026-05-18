import { useEffect, useState } from "react";

import type { SourceSelection } from "./MessageBubble";
import { normalizeWhitespace, shorten, shortId } from "../utils/sources";

type SourceDetailPanelProps = {
  selection: SourceSelection | null;
};

function stringifyDebug(selection: SourceSelection): string {
  const source = selection.source;
  const message = selection.message;
  return JSON.stringify(
    {
      metadata: message.metadata || {},
      context_used: message.context_used ?? null,
      degraded_mode: message.degraded_mode ?? null,
      degraded_reason: message.degraded_reason ?? null,
      source: {
        key: source.key,
        title: source.title,
        sourceName: source.sourceName,
        sourceUrl: source.sourceUrl,
        articleId: source.articleId,
        docId: source.docId,
        score: source.score,
        snippets: source.snippets.map((chunk) => ({
          id: chunk.id,
          score: chunk.score,
          metadata: chunk.metadata,
        })),
      },
    },
    null,
    2,
  );
}

export function SourceDetailPanel({ selection }: SourceDetailPanelProps) {
  const [viewMode, setViewMode] = useState<"summary" | "raw">("raw");

  useEffect(() => {
    setViewMode("raw");
  }, [selection?.source.key]);

  if (!selection) {
    return (
      <aside className="detailPanel empty">
        <div className="emptyIcon">◎</div>
        <h2>Chọn một nguồn</h2>
        <p>Click citation hoặc thẻ tài liệu bên dưới câu trả lời để xem chi tiết evidence.</p>
      </aside>
    );
  }

  const { source } = selection;
  const firstSnippet = source.snippets[0];
  const evidenceText = source.snippets
    .map((chunk) => normalizeWhitespace(chunk.text || ""))
    .filter(Boolean)
    .join("\n\n---\n\n");
  const visibleEvidence =
    viewMode === "summary" ? shorten(evidenceText, 900) : evidenceText;

  return (
    <aside className="detailPanel">
      <div className="detailHeader">
        <span className="infoDot">i</span>
        <span>Chi tiết tài liệu</span>
      </div>

      <h2>{source.title}</h2>

      <div className="detailChips">
        <span>{source.sourceName}</span>
        <span>Năm: {source.year || "N/A"}</span>
        <span>ID: {shortId(source.articleId || source.docId || firstSnippet?.id || "")}</span>
      </div>

      {(source.authors || source.institution) && (
        <p className="detailAuthor">
          Tác giả: <em>{source.authors || source.institution}</em>
        </p>
      )}

      <div className="detailSubhead">
        <span>Nội dung trích dẫn gốc</span>
        <div className="detailActions">
          <button
            className={viewMode === "summary" ? "active" : ""}
            onClick={() => setViewMode("summary")}
            type="button"
          >
            Tóm tắt nhanh
          </button>
          <button
            className={viewMode === "raw" ? "active" : ""}
            onClick={() => setViewMode("raw")}
            type="button"
          >
            Đoạn gốc
          </button>
        </div>
      </div>

      <div className="evidenceBox">
        <h3>{source.sectionTitle || "Đoạn evidence liên quan"}</h3>
        <p>{visibleEvidence || "Nguồn này chưa có text trích dẫn trong response."}</p>
      </div>

      <div className="evidencePreview">
        <span>Xem nhanh</span>
        <p>{shorten(evidenceText, 420)}</p>
      </div>

      <a
        className={`sourceLink ${source.sourceUrl ? "" : "disabled"}`}
        href={source.sourceUrl || undefined}
        rel="noreferrer"
        target="_blank"
      >
        Xem nguồn tài liệu
      </a>

      <details className="debugBox">
        <summary>Debug RAG</summary>
        <pre>{stringifyDebug(selection)}</pre>
      </details>
    </aside>
  );
}
