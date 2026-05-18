export type AnswerMode = "standard" | "thinking";

export type ChunkMetadata = {
  source_name?: string;
  source_id?: string;
  source_url?: string;
  title?: string;
  canonical_title?: string;
  doc_type?: string;
  section_title?: string;
  article_id?: string;
  doc_id?: string;
  institution?: string;
  authors?: string;
  author?: string;
  year?: string | number;
  specialty?: string;
  language?: string;
  [key: string]: unknown;
};

export type RetrievedChunk = {
  id: string;
  text: string;
  score?: number | null;
  metadata: ChunkMetadata;
};

export type ExternalSource = {
  id: string;
  title: string;
  url: string;
  snippet?: string;
  source_domain?: string;
};

export type ChatMetadata = {
  answer_mode?: AnswerMode | string;
  query_type?: string;
  answer_policy?: string;
  answer_style?: string;
  retrieval_mode?: string;
  coverage_level?: string;
  coverage_mode?: string;
  verification_status?: string;
  external_search_status?: string;
  timings_ms?: Record<string, number>;
  cache?: Record<string, boolean>;
  [key: string]: unknown;
};

export type ChatMessage = {
  role: "user" | "assistant";
  content: string;
  created_at?: number;
  context_used?: number;
  retrieved_chunks?: RetrievedChunk[];
  external_sources?: ExternalSource[];
  metadata?: ChatMetadata;
  duration_ms?: number;
  degraded_mode?: boolean;
  degraded_reason?: string | null;
  _local_id?: string;
  _pending?: boolean;
  _error?: boolean;
};

export type ChatResponse = {
  session_id: string;
  answer: string;
  history: ChatMessage[];
  context_used: number;
  retrieved_chunks?: RetrievedChunk[];
  metadata?: ChatMetadata;
  external_sources?: ExternalSource[];
  degraded_mode?: boolean;
  degraded_reason?: string | null;
};

export type SessionSummary = {
  id: string;
  title: string;
  updated_at?: number;
};

export type RagSource = {
  number: number;
  key: string;
  title: string;
  sourceName: string;
  sourceUrl: string;
  docType: string;
  sectionTitle: string;
  articleId: string;
  docId: string;
  institution: string;
  authors: string;
  year: string;
  score?: number | null;
  snippets: RetrievedChunk[];
};
