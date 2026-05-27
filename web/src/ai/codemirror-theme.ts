import { EditorView } from "@codemirror/view";
import { HighlightStyle, syntaxHighlighting } from "@codemirror/language";
import { tags as t } from "@lezer/highlight";

// CodeMirror theme tied to our CSS variables. Two themes (dark/light)
// so the editor's chrome reads correctly in both modes. We pull the
// computed color values via `hsl(var(--token))` strings so a runtime
// theme toggle just re-evaluates the variables — no editor remount.
//
// Use `cmThemes.dark` or `cmThemes.light` with CodeMirror's `extensions`.

const FONT_STACK =
  '"JetBrains Mono", ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace';

function makeTheme(opts: { dark: boolean }): ReadonlyArray<unknown> {
  const dark = opts.dark;
  const fg = dark ? "hsl(210 40% 96%)" : "hsl(215 28% 17%)";
  const bg = dark ? "hsl(222 40% 9%)" : "hsl(0 0% 100%)";
  const border = dark ? "hsl(217 33% 20%)" : "hsl(214 32% 88%)";
  const cursor = dark ? "hsl(187 90% 60%)" : "hsl(187 85% 43%)";
  const selectionBg = dark ? "hsl(187 90% 50% / 0.18)" : "hsl(187 85% 43% / 0.18)";
  const gutterBg = dark ? "hsl(222 40% 7%)" : "hsl(210 40% 98%)";
  const gutterFg = dark ? "hsl(217 15% 55%)" : "hsl(215 16% 47%)";
  const activeLineBg = dark ? "hsl(222 40% 13%)" : "hsl(214 32% 95%)";

  const editorTheme = EditorView.theme(
    {
      "&": {
        color: fg,
        backgroundColor: bg,
        fontFamily: FONT_STACK,
        fontSize: "13px",
      },
      ".cm-content": {
        caretColor: cursor,
        padding: "10px 0",
      },
      "&.cm-focused .cm-cursor": { borderLeftColor: cursor },
      ".cm-line": { padding: "0 12px" },
      ".cm-activeLine": { backgroundColor: activeLineBg },
      "&.cm-focused .cm-selectionBackground, ::selection, .cm-selectionBackground": {
        backgroundColor: selectionBg,
      },
      ".cm-gutters": {
        backgroundColor: gutterBg,
        color: gutterFg,
        border: "none",
        borderRight: `1px solid ${border}`,
        fontFamily: FONT_STACK,
      },
      ".cm-activeLineGutter": {
        backgroundColor: activeLineBg,
        color: fg,
      },
      ".cm-tooltip": {
        backgroundColor: bg,
        border: `1px solid ${border}`,
        borderRadius: "6px",
      },
    },
    { dark },
  );

  // Token colors — picked to harmonize with the brand palette: cyan
  // accent for keywords (it's the app's "verb" color), violet for
  // numbers/literals (subtle nod to the AI lane), warm orange for
  // strings, muted gray for comments.
  const highlight = HighlightStyle.define(
    [
      { tag: t.keyword, color: dark ? "hsl(187 90% 60%)" : "hsl(187 85% 38%)", fontWeight: "600" },
      { tag: [t.string, t.special(t.string)], color: dark ? "hsl(24 95% 65%)" : "hsl(24 95% 45%)" },
      { tag: [t.number, t.bool, t.null], color: dark ? "hsl(262 85% 75%)" : "hsl(262 70% 50%)" },
      { tag: [t.comment, t.lineComment, t.blockComment], color: dark ? "hsl(217 15% 50%)" : "hsl(215 16% 55%)", fontStyle: "italic" },
      { tag: [t.operator, t.punctuation], color: dark ? "hsl(217 15% 70%)" : "hsl(215 16% 40%)" },
      { tag: [t.function(t.variableName), t.function(t.propertyName)], color: dark ? "hsl(213 70% 70%)" : "hsl(213 51% 35%)" },
      { tag: [t.variableName, t.propertyName], color: fg },
      { tag: [t.typeName, t.className], color: dark ? "hsl(152 60% 60%)" : "hsl(152 60% 35%)" },
      { tag: t.invalid, color: dark ? "hsl(0 75% 70%)" : "hsl(0 75% 45%)" },
    ],
    { themeType: dark ? "dark" : "light" },
  );

  return [editorTheme, syntaxHighlighting(highlight)];
}

export const cmThemes = {
  dark: makeTheme({ dark: true }),
  light: makeTheme({ dark: false }),
};
