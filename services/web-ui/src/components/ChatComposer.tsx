import type { FormEvent, KeyboardEvent } from "react";

import type { AnswerMode } from "../types/rag";

type ChatComposerProps = {
  value: string;
  mode: AnswerMode;
  disabled?: boolean;
  onChange: (value: string) => void;
  onModeChange: (mode: AnswerMode) => void;
  onSubmit: () => void;
};

export function ChatComposer({
  value,
  mode,
  disabled,
  onChange,
  onModeChange,
  onSubmit,
}: ChatComposerProps) {
  const submit = (event: FormEvent) => {
    event.preventDefault();
    onSubmit();
  };

  const handleKeyDown = (event: KeyboardEvent<HTMLTextAreaElement>) => {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      onSubmit();
    }
  };

  return (
    <form className="composer" onSubmit={submit}>
      <textarea
        aria-label="Nhập câu hỏi y khoa"
        disabled={disabled}
        onChange={(event) => onChange(event.target.value)}
        onKeyDown={handleKeyDown}
        placeholder="Hỏi về bệnh học, nghiên cứu, chẩn đoán, điều trị..."
        rows={3}
        value={value}
      />
      <div className="composerFooter">
        <div className="modeToggle" aria-label="Chế độ trả lời">
          <button
            className={mode === "standard" ? "active" : ""}
            disabled={disabled}
            onClick={() => onModeChange("standard")}
            type="button"
          >
            Standard
          </button>
          <button
            className={mode === "thinking" ? "active" : ""}
            disabled={disabled}
            onClick={() => onModeChange("thinking")}
            type="button"
          >
            Thinking
          </button>
        </div>
        <div className="composerHint">
          {mode === "thinking"
            ? "Chi tiết hơn, dùng nhiều retrieval hơn."
            : "Nhanh hơn, vẫn giữ citation rõ."}
        </div>
        <button className="sendButton" disabled={disabled || !value.trim()} type="submit">
          Gửi
        </button>
      </div>
    </form>
  );
}
