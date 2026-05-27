// Centralized keyboard-shortcut registry.
//
// We want shortcuts to be:
//   - declared in one place (this file) so the help overlay can render
//     a definitive cheat-sheet without scraping the codebase;
//   - keyed by stable id (not by chord string) so the chord can change
//     without breaking the help/UI;
//   - safe in editable contexts — a shortcut should not fire while the
//     user is typing into an input/textarea/contenteditable, unless it
//     opts in via `allowInEditable: true` (Cmd/Ctrl+K does opt in so it
//     toggles the assistant even from the composer).
//
// Chord syntax: a chord is a string like `"mod+k"`, `"?"`, `"g e"`. The
// `mod` token matches metaKey on Mac and ctrlKey elsewhere. Sequences
// (whitespace-separated) require the keys pressed within ~1s of each
// other; this is how `g e` (go executive) works without colliding with
// the single-key `?` or `/` chords.

export type ShortcutScope = "global" | "ai";

export interface ShortcutSpec {
  /** Stable id. Used as a React key and to look up handlers. */
  id: string;
  /** Chord string, e.g. "mod+k", "?", "g e". */
  chord: string;
  /** Human-readable label for the help overlay. */
  label: string;
  /** Grouping for the help overlay. */
  group: "Navigation" | "Assistant" | "General";
  /** Whether the shortcut should fire while focus is in an editable
   * element. Defaults to false. Cmd/Ctrl+K opts in. */
  allowInEditable?: boolean;
}

export const SHORTCUTS: ReadonlyArray<ShortcutSpec> = [
  // General
  {
    id: "help",
    chord: "?",
    label: "Show keyboard shortcuts",
    group: "General",
  },
  {
    id: "search",
    chord: "/",
    label: "Focus search / quick command",
    group: "General",
  },

  // Assistant
  {
    id: "toggle-assistant",
    chord: "mod+k",
    label: "Toggle AI assistant",
    group: "Assistant",
    allowInEditable: true,
  },

  // Navigation (vim-style chord sequences)
  { id: "go-executive", chord: "g e", label: "Go to Executive", group: "Navigation" },
  { id: "go-operations", chord: "g o", label: "Go to Operations", group: "Navigation" },
  { id: "go-maintenance", chord: "g m", label: "Go to Maintenance", group: "Navigation" },
  { id: "go-risk", chord: "g r", label: "Go to Risk & behavior", group: "Navigation" },
  { id: "go-safety", chord: "g s", label: "Go to Safety scorecard", group: "Navigation" },
  { id: "go-alerts", chord: "g a", label: "Go to Predictive alerts", group: "Navigation" },
  { id: "go-efficiency", chord: "g f", label: "Go to Efficiency", group: "Navigation" },
  { id: "go-ai", chord: "g i", label: "Go to AI assistant", group: "Navigation" },
];

/** Look up a shortcut's display chord by id (for tooltips/footers). */
export function chordOf(id: string): string {
  return SHORTCUTS.find((s) => s.id === id)?.chord ?? "";
}

/** Render a chord for the UI. Splits on whitespace so the keys map
 *  onto separate <Kbd> elements. Replaces "mod" with the platform glyph
 *  at render time. */
export function renderChord(chord: string, isMac: boolean): string[] {
  return chord.split(/\s+/).map((token) =>
    token
      .split("+")
      .map((k) => (k === "mod" ? (isMac ? "⌘" : "Ctrl") : prettyKey(k)))
      .join("+"),
  );
}

function prettyKey(k: string): string {
  if (k === "shift") return "Shift";
  if (k === "alt") return "Alt";
  if (k === "enter") return "Enter";
  if (k === "esc") return "Esc";
  if (k.length === 1) return k.toUpperCase();
  return k;
}
