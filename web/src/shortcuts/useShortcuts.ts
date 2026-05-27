// Hook that subscribes to a set of shortcut chords and fires the
// matching handler. Each handler receives the original KeyboardEvent so
// it can preventDefault if it wants to.
//
// The hook listens on `window` once per mount; multiple `useShortcuts`
// calls in different components compose cleanly because each has its
// own subscription. Order is undefined across subscribers, but a given
// chord only ever has one handler in practice (handlers are paired with
// shortcut ids via the registry).
//
// A small in-hook "pending sequence" buffer handles two-token chords
// like `g e`: after pressing `g`, we remember that for ~1s, and if the
// next keystroke matches `<pending> <key>` we fire. Pressing any
// modifier or moving focus into an editable element clears the buffer.

import { useEffect, useRef } from "react";
import { SHORTCUTS, type ShortcutSpec } from "./registry";

export type ShortcutHandler = (e: KeyboardEvent) => void;
export type ShortcutMap = Partial<Record<string, ShortcutHandler>>;

const SEQUENCE_TIMEOUT_MS = 1000;

function isEditable(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false;
  const tag = target.tagName;
  if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return true;
  if (target.isContentEditable) return true;
  return false;
}

function tokenFor(e: KeyboardEvent): string | null {
  // Ignore lone modifier keystrokes.
  if (e.key === "Control" || e.key === "Meta" || e.key === "Shift" || e.key === "Alt") {
    return null;
  }
  const parts: string[] = [];
  if (e.metaKey || e.ctrlKey) parts.push("mod");
  if (e.shiftKey && e.key.length > 1) parts.push("shift");
  if (e.altKey) parts.push("alt");
  const key = e.key.length === 1 ? e.key.toLowerCase() : e.key.toLowerCase();
  parts.push(key);
  return parts.join("+");
}

interface PendingChord {
  prefix: string;
  expiresAt: number;
}

export function useShortcuts(handlers: ShortcutMap): void {
  const handlersRef = useRef<ShortcutMap>(handlers);
  handlersRef.current = handlers;

  useEffect(() => {
    let pending: PendingChord | null = null;

    function fire(spec: ShortcutSpec, e: KeyboardEvent) {
      const h = handlersRef.current[spec.id];
      if (!h) return;
      h(e);
    }

    function onKey(e: KeyboardEvent) {
      const token = tokenFor(e);
      if (!token) return;

      // First: full single-token match.
      const editable = isEditable(e.target);
      const now = Date.now();

      // Try to close an in-flight sequence chord first.
      if (pending && pending.expiresAt >= now) {
        const candidate = `${pending.prefix} ${token}`;
        const seqSpec = SHORTCUTS.find(
          (s) => s.chord === candidate && (!editable || s.allowInEditable),
        );
        pending = null;
        if (seqSpec) {
          e.preventDefault();
          fire(seqSpec, e);
          return;
        }
        // Fall through: maybe this lone token is itself a chord.
      } else {
        pending = null;
      }

      // Single-token chord (incl. `mod+k`).
      const spec = SHORTCUTS.find(
        (s) =>
          s.chord === token &&
          !s.chord.includes(" ") &&
          (!editable || s.allowInEditable),
      );
      if (spec) {
        e.preventDefault();
        fire(spec, e);
        return;
      }

      // Maybe this is the first token of a sequence chord.
      if (editable) return;
      const isSeqPrefix = SHORTCUTS.some(
        (s) => s.chord.includes(" ") && s.chord.split(/\s+/)[0] === token,
      );
      if (isSeqPrefix) {
        pending = { prefix: token, expiresAt: now + SEQUENCE_TIMEOUT_MS };
      }
    }

    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);
}
