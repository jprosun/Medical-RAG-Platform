import type { ExternalSource, RagSource, RetrievedChunk } from "../types/rag";

export function shortId(value: string, length = 8): string {
  return value ? value.slice(0, length) : "N/A";
}

export function normalizeWhitespace(text: string): string {
  return (text || "").replace(/\s+/g, " ").trim();
}

export function shorten(text: string, limit = 680): string {
  const normalized = normalizeWhitespace(text);
  if (normalized.length <= limit) return normalized;
  return `${normalized.slice(0, limit).replace(/\s+\S*$/, "")}...`;
}

function sourceKey(chunk: RetrievedChunk): string {
  const md = chunk.metadata || {};
  if (md.source_url) return `url:${md.source_url}`;
  if (md.article_id) return `article:${md.article_id}`;
  if (md.doc_id) return `doc:${md.doc_id}`;
  const title = String(md.canonical_title || md.title || "").toLowerCase();
  const source = String(md.source_name || md.source_id || "").toLowerCase();
  return `title:${source}:${title || chunk.id}`;
}

export function buildRagSources(chunks: RetrievedChunk[] = []): RagSource[] {
  const sources: RagSource[] = [];
  const index = new Map<string, number>();

  for (const chunk of chunks) {
    const md = chunk.metadata || {};
    const key = sourceKey(chunk);
    let sourceIndex = index.get(key);
    if (sourceIndex === undefined) {
      sourceIndex = sources.length;
      index.set(key, sourceIndex);
      sources.push({
        number: sources.length + 1,
        key,
        title: String(md.title || md.canonical_title || "Nguồn không có tiêu đề"),
        sourceName: String(md.source_name || md.source_id || "RAG"),
        sourceUrl: String(md.source_url || ""),
        docType: String(md.doc_type || ""),
        sectionTitle: String(md.section_title || ""),
        articleId: String(md.article_id || ""),
        docId: String(md.doc_id || chunk.id || ""),
        institution: String(md.institution || ""),
        authors: String(md.authors || md.author || md.institution || ""),
        year: md.year ? String(md.year) : "N/A",
        score: chunk.score,
        snippets: [],
      });
    }
    if (sources[sourceIndex].snippets.length < 4) {
      sources[sourceIndex].snippets.push(chunk);
    }
  }

  return sources.slice(0, 12);
}

export function citationTokenForSource(source: RagSource): string {
  return `[${source.number}]`;
}

export function externalTokenForSource(source: ExternalSource): string {
  return `[${source.id || "E"}]`;
}
