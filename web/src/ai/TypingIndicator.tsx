import { Bot } from "lucide-react";
import { cn } from "@/lib/cn";

interface TypingIndicatorProps {
  /** Optional caption shown next to the dots — "Generating SQL…", "Running query…". */
  label?: string;
  className?: string;
}

// Three-dot pulse + optional caption. Use as a placeholder bubble while
// the assistant is working. Animation cadence matches `pulse-soft` in
// tailwind.config.ts so it harmonizes with the AssistantFAB pulse.
export function TypingIndicator({
  label = "Thinking…",
  className,
}: TypingIndicatorProps) {
  return (
    <div className={cn("flex items-start gap-3", className)}>
      <span className="flex size-7 shrink-0 items-center justify-center rounded-full bg-gradient-ai text-white">
        <Bot className="size-3.5" />
      </span>
      <div className="flex items-center gap-2 rounded-2xl rounded-tl-sm border border-ai/20 bg-ai/5 px-4 py-3">
        <span className="flex items-end gap-1">
          <span
            className="size-1.5 rounded-full bg-ai animate-pulse-soft"
            style={{ animationDelay: "0ms" }}
          />
          <span
            className="size-1.5 rounded-full bg-ai animate-pulse-soft"
            style={{ animationDelay: "150ms" }}
          />
          <span
            className="size-1.5 rounded-full bg-ai animate-pulse-soft"
            style={{ animationDelay: "300ms" }}
          />
        </span>
        <span className="text-xs text-muted-foreground">{label}</span>
      </div>
    </div>
  );
}
