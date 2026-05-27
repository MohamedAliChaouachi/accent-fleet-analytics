import { useEffect, useRef } from "react";
import { AlertTriangle, Loader2 } from "lucide-react";
import { ChatMessage, MessageMetaChip } from "./ChatMessage";
import { StreamingMessage } from "./StreamingMessage";
import { TypingIndicator } from "./TypingIndicator";
import { SqlCodeBlock } from "./SqlCodeBlock";
import { ResultsTable } from "./ResultsTable";
import { ExplanationPanel } from "./ExplanationPanel";
import { deriveConfidence } from "./ConfidenceBadge";
import { QueryFeedback, type FeedbackValue } from "./QueryFeedback";
import { prettyAIError, type DisplayMessage } from "./useAIChat";
import { cn } from "@/lib/cn";

interface MessageListProps {
  messages: ReadonlyArray<DisplayMessage>;
  loading: boolean;
  error: Error | null;
  /** When true, freshly-arrived assistant messages animate in via
   * typewriter. Restored history is rendered instantly regardless. */
  animateLatest?: boolean;
  /** Per-message feedback state, keyed by message id. */
  feedback?: Record<number, FeedbackValue>;
  onFeedbackChange?: (id: number, value: FeedbackValue, comment?: string) => void;
  /** Compact mode for the slide-out panel (smaller padding). */
  compact?: boolean;
  className?: string;
}

// Renders the conversation transcript. Auto-scrolls to bottom when new
// messages arrive or while the assistant is typing. Auto-scroll yields
// to the user if they've scrolled up to read history.
export function MessageList({
  messages,
  loading,
  error,
  animateLatest = true,
  feedback,
  onFeedbackChange,
  compact = false,
  className,
}: MessageListProps) {
  const scrollerRef = useRef<HTMLDivElement | null>(null);
  const stickToBottomRef = useRef(true);

  // Track whether the user has scrolled up — when they have, we stop
  // auto-following. As soon as they return to the bottom, resume.
  function onScroll() {
    const el = scrollerRef.current;
    if (!el) return;
    const slack = 24; // px tolerance
    const atBottom =
      el.scrollHeight - el.scrollTop - el.clientHeight <= slack;
    stickToBottomRef.current = atBottom;
  }

  useEffect(() => {
    if (!stickToBottomRef.current) return;
    const el = scrollerRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [messages.length, loading]);

  const lastAssistantId = (() => {
    for (let i = messages.length - 1; i >= 0; i--) {
      if (messages[i]!.role === "assistant") return messages[i]!.id;
    }
    return null;
  })();

  return (
    <div
      ref={scrollerRef}
      onScroll={onScroll}
      className={cn(
        "flex-1 min-h-0 overflow-y-auto",
        compact ? "px-3 py-4" : "px-4 py-6",
        className,
      )}
    >
      <div
        className={cn(
          "mx-auto flex flex-col gap-5",
          compact ? "max-w-full" : "max-w-3xl",
        )}
      >
        {messages.map((m) => {
          if (m.role === "user") {
            return (
              <ChatMessage key={m.id} role="user" timestamp={m.createdAt}>
                <p className="whitespace-pre-wrap text-sm leading-relaxed">
                  {m.content}
                </p>
              </ChatMessage>
            );
          }
          const { response } = m;
          const isLatest = m.id === lastAssistantId;
          const animate = animateLatest && isLatest;
          const confidence = deriveConfidence({
            rowCount: response.row_count,
            summaryLength: response.summary.length,
          });
          return (
            <ChatMessage
              key={m.id}
              role="assistant"
              timestamp={m.createdAt}
              footer={
                <>
                  <MessageMetaChip>
                    {response.row_count}{" "}
                    {response.row_count === 1 ? "row" : "rows"}
                  </MessageMetaChip>
                  <MessageMetaChip>{response.elapsed_ms} ms</MessageMetaChip>
                  <MessageMetaChip>
                    {response.provider}/{response.model}
                  </MessageMetaChip>
                  {onFeedbackChange ? (
                    <QueryFeedback
                      value={feedback?.[m.id] ?? null}
                      onChange={(v, c) => onFeedbackChange(m.id, v, c)}
                    />
                  ) : null}
                </>
              }
            >
              <div className="flex flex-col gap-3">
                <StreamingMessage text={response.summary} animate={animate} />
                <SqlCodeBlock
                  sql={response.sql}
                  source="ai"
                  confidence={confidence}
                />
                {!compact ? (
                  <ExplanationPanel sql={response.sql} />
                ) : null}
                <ResultsTable
                  rows={response.rows}
                  columns={response.columns}
                  chartType={response.chart_type}
                />
              </div>
            </ChatMessage>
          );
        })}

        {loading ? <TypingIndicator label="Generating SQL…" /> : null}

        {error ? <ErrorBubble error={error} /> : null}
      </div>
    </div>
  );
}

function ErrorBubble({ error }: { error: Error }) {
  const { title, hint } = prettyAIError(error);
  return (
    <div className="flex items-start gap-3">
      <span className="flex size-7 shrink-0 items-center justify-center rounded-full bg-destructive/10 text-destructive">
        <AlertTriangle className="size-3.5" />
      </span>
      <div className="rounded-2xl rounded-tl-sm border border-destructive/30 bg-destructive/5 px-4 py-3 text-sm">
        <p className="font-medium text-foreground">{title}</p>
        <p className="mt-1 whitespace-pre-wrap text-xs text-muted-foreground">
          {error.message}
        </p>
        <p className="mt-2 text-2xs text-muted-foreground/80">{hint}</p>
      </div>
    </div>
  );
}

export function LoadingShell() {
  return (
    <div className="flex h-full items-center justify-center text-muted-foreground">
      <Loader2 className="mr-2 size-4 animate-spin" />
      <span className="text-sm">Loading conversation…</span>
    </div>
  );
}
