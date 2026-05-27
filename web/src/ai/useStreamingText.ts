import { useEffect, useRef, useState } from "react";

// Client-side typewriter for AI responses.
//
// The backend currently returns the full summary in one POST response,
// so we simulate streaming by progressively revealing the text. Two
// strategies in one hook:
//
//   - "tokens": split on whitespace, reveal one word per tick.
//     Reads naturally; faster than a per-char crawl on long responses.
//
//   - "chars": one character per tick. Used for short text (e.g.
//     headers) where the word-level granularity feels chunky.
//
// When the backend grows real Server-Sent Events / WebSocket streaming,
// swap this for a hook that takes the stream directly — the consumer
// component contract (`{ text, isStreaming }`) stays the same.

interface Options {
  /** Whether to actually animate. If false, the full text is revealed
   * immediately. Useful for messages restored from localStorage so old
   * conversations don't re-type themselves on every nav. */
  animate?: boolean;
  /** Reveal mode. Defaults to "tokens". */
  mode?: "tokens" | "chars";
  /** Tokens-per-second when mode="tokens", chars-per-second when "chars".
   * Defaults: 40 tps for tokens (~150 wpm — fast read), 80 cps for chars. */
  speed?: number;
  /** Called once when the typewriter finishes. */
  onComplete?: () => void;
}

interface State {
  text: string;
  isStreaming: boolean;
}

export function useStreamingText(full: string, opts: Options = {}): State {
  const { animate = true, mode = "tokens", speed, onComplete } = opts;

  // Resolve the per-tick delay from the speed setting once.
  const intervalMs = mode === "tokens" ? 1000 / (speed ?? 40) : 1000 / (speed ?? 80);

  const [shown, setShown] = useState<string>(animate ? "" : full);
  const [streaming, setStreaming] = useState<boolean>(animate && full.length > 0);

  // Capture the latest onComplete in a ref so changing the prop between
  // renders doesn't restart the animation.
  const completeRef = useRef(onComplete);
  completeRef.current = onComplete;

  useEffect(() => {
    if (!animate || full.length === 0) {
      setShown(full);
      setStreaming(false);
      return;
    }

    setShown("");
    setStreaming(true);

    // Pre-compute the reveal schedule so we don't re-split on every tick.
    const chunks =
      mode === "tokens" ? splitKeepWhitespace(full) : Array.from(full);
    let i = 0;

    const handle = window.setInterval(() => {
      i += 1;
      if (i >= chunks.length) {
        setShown(full);
        setStreaming(false);
        completeRef.current?.();
        window.clearInterval(handle);
      } else {
        setShown(chunks.slice(0, i).join(""));
      }
    }, intervalMs);

    return () => window.clearInterval(handle);
  }, [full, animate, mode, intervalMs]);

  return { text: shown, isStreaming: streaming };
}

// Split text into ["word", " ", "word", "\n", "word"…] so the join
// preserves original whitespace (newlines, indentation, double spaces).
// A naive split(" ") would collapse `"a  b"` to `"a b"` and strip the
// trailing newline that paragraph breaks depend on.
function splitKeepWhitespace(s: string): string[] {
  const out: string[] = [];
  let buf = "";
  let inWs = /\s/.test(s[0] ?? "");
  for (const ch of s) {
    const ws = /\s/.test(ch);
    if (ws === inWs) {
      buf += ch;
    } else {
      out.push(buf);
      buf = ch;
      inWs = ws;
    }
  }
  if (buf) out.push(buf);
  return out;
}
