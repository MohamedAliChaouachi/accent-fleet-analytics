import type { ReactNode } from "react";
import { Bot, User } from "lucide-react";
import { cn } from "@/lib/cn";

interface ChatMessageProps {
  role: "user" | "assistant";
  /** ISO timestamp or epoch ms. Optional — when present, rendered as a
   * relative "2m ago" in the footer. */
  timestamp?: number;
  /** Optional avatar override (defaults to icon). */
  avatar?: ReactNode;
  /** Optional footer (used for response metadata, feedback buttons). */
  footer?: ReactNode;
  children: ReactNode;
  className?: string;
}

// Generic chat bubble layout: avatar on the left for AI, on the right
// for the user. Content slot is the children; specific message types
// (StreamingMessage, SqlCodeBlock, ResultsTable) compose inside.
export function ChatMessage({
  role,
  timestamp,
  avatar,
  footer,
  children,
  className,
}: ChatMessageProps) {
  const isUser = role === "user";

  const avatarEl =
    avatar ??
    (isUser ? (
      <span className="flex size-7 shrink-0 items-center justify-center rounded-full bg-secondary text-secondary-foreground">
        <User className="size-3.5" />
      </span>
    ) : (
      <span className="flex size-7 shrink-0 items-center justify-center rounded-full bg-gradient-ai text-white shadow-sm">
        <Bot className="size-3.5" />
      </span>
    ));

  return (
    <div
      className={cn(
        "flex items-start gap-3",
        isUser && "flex-row-reverse",
        className,
      )}
    >
      {avatarEl}
      <div
        className={cn(
          "flex max-w-[88%] min-w-0 flex-col gap-2",
          isUser && "items-end",
        )}
      >
        <div
          className={cn(
            "rounded-2xl px-4 py-3 shadow-sm",
            isUser
              ? "rounded-tr-sm bg-primary text-primary-foreground"
              : "rounded-tl-sm border border-border bg-card text-card-foreground",
          )}
        >
          {children}
        </div>
        {(footer || timestamp) && (
          <div
            className={cn(
              "flex items-center gap-2 px-1 text-2xs text-muted-foreground",
              isUser && "flex-row-reverse",
            )}
          >
            {timestamp ? <span>{formatRelative(timestamp)}</span> : null}
            {footer}
          </div>
        )}
      </div>
    </div>
  );
}

// Small reusable footer chip — used for tokens/rows/time metadata.
export function MessageMetaChip({
  children,
  className,
}: {
  children: ReactNode;
  className?: string;
}) {
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 rounded-md bg-muted px-1.5 py-0.5 text-2xs font-medium text-muted-foreground",
        className,
      )}
    >
      {children}
    </span>
  );
}

function formatRelative(ts: number): string {
  const diff = Date.now() - ts;
  const min = Math.floor(diff / 60_000);
  if (min < 1) return "just now";
  if (min < 60) return `${min}m ago`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr}h ago`;
  const day = Math.floor(hr / 24);
  if (day === 1) return "yesterday";
  if (day < 7) return `${day}d ago`;
  return new Date(ts).toLocaleDateString();
}
