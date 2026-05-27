import {
  forwardRef,
  useEffect,
  useImperativeHandle,
  useRef,
  type FormEvent,
  type KeyboardEvent,
} from "react";
import { ArrowUp, Loader2, Square } from "lucide-react";
import { cn } from "@/lib/cn";
import { Button } from "@/components/ui/Button";
import { Kbd } from "@/components/ui/Kbd";

export interface ChatComposerHandle {
  focus: () => void;
  setValue: (s: string) => void;
}

interface ChatComposerProps {
  value: string;
  onChange: (v: string) => void;
  onSubmit: () => void;
  /** Cancel an in-flight request. When provided AND `loading`, the send
   * button becomes a stop button. */
  onCancel?: () => void;
  loading?: boolean;
  disabled?: boolean;
  placeholder?: string;
  /** Banner above the textarea — for inline errors or tenant warnings. */
  banner?: React.ReactNode;
  /** Compact mode for the slide-out panel (smaller padding, smaller font). */
  compact?: boolean;
  className?: string;
}

// Auto-grow textarea with Enter-to-send / Shift+Enter newline.
// ⌘↵ / Ctrl↵ also submits — matches the brief's keyboard shortcuts.
export const ChatComposer = forwardRef<ChatComposerHandle, ChatComposerProps>(
  (
    {
      value,
      onChange,
      onSubmit,
      onCancel,
      loading = false,
      disabled = false,
      placeholder = "Ask anything about your fleet…",
      banner,
      compact = false,
      className,
    },
    ref,
  ) => {
    const taRef = useRef<HTMLTextAreaElement | null>(null);

    useImperativeHandle(ref, () => ({
      focus: () => taRef.current?.focus(),
      setValue: (s: string) => {
        onChange(s);
        // Defer focus to after the value lands so the caret sits at the
        // right of the inserted text.
        requestAnimationFrame(() => taRef.current?.focus());
      },
    }));

    // Auto-grow: reset to single-line height first, then snap to scrollHeight.
    useEffect(() => {
      const el = taRef.current;
      if (!el) return;
      el.style.height = "auto";
      el.style.height = `${Math.min(el.scrollHeight, 160)}px`;
    }, [value]);

    function submit(e: FormEvent) {
      e.preventDefault();
      if (loading || disabled || !value.trim()) return;
      onSubmit();
    }

    function onKey(e: KeyboardEvent<HTMLTextAreaElement>) {
      const mod = e.metaKey || e.ctrlKey;
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        if (!loading && !disabled && value.trim()) onSubmit();
      } else if (mod && e.key === "Enter") {
        e.preventDefault();
        if (!loading && !disabled && value.trim()) onSubmit();
      }
    }

    const empty = !value.trim();
    const showStop = loading && onCancel;

    return (
      <form onSubmit={submit} className={cn("flex flex-col gap-2", className)}>
        {banner ? <div>{banner}</div> : null}
        <div
          className={cn(
            "group/composer relative flex items-end gap-2 rounded-2xl border bg-card transition-colors",
            "focus-within:border-ai/50 focus-within:shadow-ai-glow/30",
            compact ? "p-1.5" : "p-2",
            disabled
              ? "border-border opacity-60"
              : "border-border hover:border-ring/60",
          )}
        >
          <textarea
            ref={taRef}
            value={value}
            onChange={(e) => onChange(e.target.value)}
            onKeyDown={onKey}
            disabled={disabled}
            placeholder={placeholder}
            rows={1}
            aria-label="Message"
            className={cn(
              "block max-h-40 flex-1 resize-none bg-transparent text-foreground placeholder:text-muted-foreground",
              "focus:outline-none disabled:cursor-not-allowed",
              compact ? "min-h-[2rem] px-2 py-1.5 text-sm" : "min-h-[2.5rem] px-2 py-2 text-sm",
            )}
          />
          {showStop ? (
            <Button
              type="button"
              variant="outline"
              size="icon"
              onClick={onCancel}
              aria-label="Stop generating"
              className="shrink-0 rounded-xl"
            >
              <Square className="size-3.5 fill-current" />
            </Button>
          ) : (
            <Button
              type="submit"
              variant={empty ? "secondary" : "ai"}
              size="icon"
              disabled={empty || disabled || loading}
              aria-label="Send"
              className="shrink-0 rounded-xl"
            >
              {loading ? (
                <Loader2 className="size-4 animate-spin" />
              ) : (
                <ArrowUp className="size-4" />
              )}
            </Button>
          )}
        </div>
        <p className="flex items-center justify-center gap-2 text-2xs text-muted-foreground">
          <Kbd>Enter</Kbd> to send
          <span className="opacity-50">·</span>
          <Kbd>Shift</Kbd>+<Kbd>Enter</Kbd> for a new line
        </p>
      </form>
    );
  },
);
ChatComposer.displayName = "ChatComposer";
