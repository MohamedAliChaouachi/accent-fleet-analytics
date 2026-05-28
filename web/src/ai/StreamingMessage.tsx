import { useEffect, useMemo } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { cn } from "@/lib/cn";
import { useStreamingText } from "./useStreamingText";

interface StreamingMessageProps {
  text: string;
  /** When true, reveal the text token-by-token. When false (e.g. for
   * restored conversations), show the full text immediately. */
  animate?: boolean;
  /** Callback when the typewriter finishes. */
  onComplete?: () => void;
  className?: string;
}

// Renders the assistant's natural-language summary with a typewriter
// effect and Markdown support. The blinking caret only shows while the
// text is still streaming, then fades out — gives the bubble a clear
// "I'm done" signal without a separate state.
export function StreamingMessage({
  text,
  animate = true,
  onComplete,
  className,
}: StreamingMessageProps) {
  const { text: shown, isStreaming } = useStreamingText(text, {
    animate,
    onComplete,
  });

  // Markdown renderer for the shown text. Memoize the components mapping
  // since react-markdown re-renders on every text change and we don't
  // want to rebuild this object 200x during a stream.
  const components = useMemo(
    () => ({
      p: ({ children, ...props }: React.HTMLAttributes<HTMLParagraphElement>) => (
        <p className="mb-2 last:mb-0 leading-relaxed" {...props}>
          {children}
        </p>
      ),
      a: ({ children, ...props }: React.AnchorHTMLAttributes<HTMLAnchorElement>) => (
        <a
          className="text-accent underline underline-offset-2 hover:text-accent/80"
          target="_blank"
          rel="noreferrer"
          {...props}
        >
          {children}
        </a>
      ),
      strong: ({ children, ...props }: React.HTMLAttributes<HTMLElement>) => (
        <strong className="font-semibold text-foreground" {...props}>
          {children}
        </strong>
      ),
      code: ({ children, ...props }: React.HTMLAttributes<HTMLElement>) => (
        <code
          className="rounded bg-muted px-1 py-0.5 font-mono text-2xs text-foreground"
          {...props}
        >
          {children}
        </code>
      ),
      ul: ({ children, ...props }: React.HTMLAttributes<HTMLUListElement>) => (
        <ul className="my-2 list-disc pl-5 space-y-1" {...props}>
          {children}
        </ul>
      ),
      ol: ({ children, ...props }: React.OlHTMLAttributes<HTMLOListElement>) => (
        <ol className="my-2 list-decimal pl-5 space-y-1" {...props}>
          {children}
        </ol>
      ),
    }),
    [],
  );

  // Track when the streaming completes for non-effect consumers (avoid
  // setting state during render in the parent).
  useEffect(() => {
    /* no-op; useStreamingText handles its own completion */
  }, [isStreaming]);

  return (
    <div className={cn("text-sm text-foreground", className)}>
      <ReactMarkdown remarkPlugins={[remarkGfm]} components={components}>
        {shown || "\u200B"}
      </ReactMarkdown>
      {isStreaming ? (
        <span
          aria-hidden
          className="ml-0.5 inline-block h-3.5 w-1 -mb-0.5 bg-ai animate-caret-blink"
        />
      ) : null}
    </div>
  );
}
