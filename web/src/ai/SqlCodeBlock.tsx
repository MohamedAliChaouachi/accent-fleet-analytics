import { useCallback, useMemo, useState } from "react";
import CodeMirror from "@uiw/react-codemirror";
import { sql, PostgreSQL } from "@codemirror/lang-sql";
import { EditorView } from "@codemirror/view";
import {
  Check,
  Copy,
  Database,
  Pencil,
  Play,
  RotateCcw,
  Sparkles,
  X,
} from "lucide-react";
import { useTheme } from "@/theme/ThemeProvider";
import { cn } from "@/lib/cn";
import { Button } from "@/components/ui/Button";
import { Badge } from "@/components/ui/Badge";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/Tooltip";
import { ConfidenceBadge, type Confidence } from "./ConfidenceBadge";
import { cmThemes } from "./codemirror-theme";

interface SqlCodeBlockProps {
  /** The generated SQL. */
  sql: string;
  /** Mark as AI-generated so the header gets the violet AI marker. */
  source?: "ai" | "user";
  /** Optional confidence shown next to the AI marker. */
  confidence?: Confidence;
  /** Optional list of schema objects the AI referenced. */
  tablesUsed?: ReadonlyArray<string>;
  /** Read-only by default. When the user clicks Edit we flip to writable. */
  readOnly?: boolean;
  /** Called when the user clicks Run (or hits ⌘↵ inside an editable block). */
  onRun?: (sql: string) => void;
  /** Called when the user commits an edit (toggle off edit mode). */
  onEdit?: (newSql: string) => void;
  /** Show line numbers gutter. Defaults true. */
  showLineNumbers?: boolean;
  className?: string;
}

// SQL code block with CodeMirror. Three states:
//
//   - read (default): displays the SQL with header chrome (copy / edit /
//     run). No gutter chrome interactions; cursor still works for select.
//
//   - edit: header swaps in Save + Cancel; editor becomes writable, ⌘↵
//     runs whatever the user has typed.
//
// We don't expose the raw textarea anymore — CodeMirror handles all
// keyboard, selection, and accessibility behavior, including IME input
// for non-Latin keyboards.
export function SqlCodeBlock({
  sql: initialSql,
  source = "ai",
  confidence,
  tablesUsed,
  readOnly = true,
  onRun,
  onEdit,
  showLineNumbers = true,
  className,
}: SqlCodeBlockProps) {
  const { resolvedTheme } = useTheme();
  const [editing, setEditing] = useState(!readOnly);
  const [draft, setDraft] = useState(initialSql);
  const [copied, setCopied] = useState(false);

  const extensions = useMemo(
    () => [
      sql({ dialect: PostgreSQL, upperCaseKeywords: true }),
      EditorView.lineWrapping,
      // ⌘↵ runs the current SQL — works in both read and edit modes,
      // since CodeMirror still owns the focus surface.
      EditorView.domEventHandlers({
        keydown(event, view) {
          const mod = event.metaKey || event.ctrlKey;
          if (mod && event.key === "Enter") {
            event.preventDefault();
            onRun?.(view.state.doc.toString());
            return true;
          }
          return false;
        },
      }),
    ],
    [onRun],
  );

  const theme = resolvedTheme === "dark" ? cmThemes.dark : cmThemes.light;

  const handleCopy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(editing ? draft : initialSql);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1500);
    } catch {
      // Clipboard permission denied or unsupported context (http://) —
      // fall back to selecting the whole text so the user can copy
      // manually with ⌘C.
      const el = document.activeElement;
      if (el instanceof HTMLElement) el.blur();
    }
  }, [draft, editing, initialSql]);

  const handleEditToggle = useCallback(() => {
    if (editing) {
      // Cancel: revert to the original.
      setDraft(initialSql);
      setEditing(false);
    } else {
      setEditing(true);
    }
  }, [editing, initialSql]);

  const handleSave = useCallback(() => {
    onEdit?.(draft);
    setEditing(false);
  }, [draft, onEdit]);

  const handleRun = useCallback(() => {
    onRun?.(editing ? draft : initialSql);
  }, [draft, editing, initialSql, onRun]);

  const handleReset = useCallback(() => {
    setDraft(initialSql);
  }, [initialSql]);

  const dirty = editing && draft !== initialSql;
  const isAi = source === "ai";

  return (
    <div
      className={cn(
        "overflow-hidden rounded-lg border bg-card",
        isAi ? "border-ai/25" : "border-border",
        className,
      )}
    >
      {/* Header */}
      <div
        className={cn(
          "flex flex-wrap items-center justify-between gap-2 border-b px-3 py-2",
          isAi
            ? "border-ai/20 bg-ai/5"
            : "border-border bg-muted/40",
        )}
      >
        <div className="flex items-center gap-2">
          {isAi ? (
            <span className="flex items-center gap-1.5 text-xs font-medium text-ai">
              <Sparkles className="size-3.5" />
              <span>SQL · AI generated</span>
            </span>
          ) : (
            <span className="flex items-center gap-1.5 text-xs font-medium text-muted-foreground">
              <Database className="size-3.5" />
              <span>SQL</span>
            </span>
          )}
          {confidence ? <ConfidenceBadge confidence={confidence} /> : null}
          {tablesUsed && tablesUsed.length > 0 ? (
            <Tooltip>
              <TooltipTrigger asChild>
                <Badge variant="outline" className="cursor-help gap-1">
                  <Database className="size-3" />
                  {tablesUsed.length} table{tablesUsed.length === 1 ? "" : "s"}
                </Badge>
              </TooltipTrigger>
              <TooltipContent>
                <div className="font-mono text-2xs">
                  {tablesUsed.map((t) => (
                    <div key={t}>{t}</div>
                  ))}
                </div>
              </TooltipContent>
            </Tooltip>
          ) : null}
        </div>

        <div className="flex items-center gap-1">
          {editing ? (
            <>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button
                    variant="ghost"
                    size="icon-sm"
                    onClick={handleReset}
                    disabled={!dirty}
                    aria-label="Reset to original"
                  >
                    <RotateCcw className="size-3.5" />
                  </Button>
                </TooltipTrigger>
                <TooltipContent>Reset</TooltipContent>
              </Tooltip>
              <Button
                variant="ghost"
                size="sm"
                onClick={handleEditToggle}
                className="h-7 gap-1 px-2 text-xs"
              >
                <X className="size-3" /> Cancel
              </Button>
              <Button
                variant="primary"
                size="sm"
                onClick={handleSave}
                disabled={!dirty}
                className="h-7 gap-1 px-2 text-xs"
              >
                <Check className="size-3" /> Save
              </Button>
            </>
          ) : (
            <>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button
                    variant="ghost"
                    size="icon-sm"
                    onClick={handleCopy}
                    aria-label="Copy SQL"
                  >
                    {copied ? (
                      <Check className="size-3.5 text-success" />
                    ) : (
                      <Copy className="size-3.5" />
                    )}
                  </Button>
                </TooltipTrigger>
                <TooltipContent>
                  {copied ? "Copied!" : "Copy SQL"}
                </TooltipContent>
              </Tooltip>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button
                    variant="ghost"
                    size="icon-sm"
                    onClick={handleEditToggle}
                    aria-label="Edit SQL"
                  >
                    <Pencil className="size-3.5" />
                  </Button>
                </TooltipTrigger>
                <TooltipContent>Edit</TooltipContent>
              </Tooltip>
            </>
          )}
          {onRun ? (
            <Button
              variant={isAi ? "ai" : "primary"}
              size="sm"
              onClick={handleRun}
              className="h-7 gap-1 px-2 text-xs"
            >
              <Play className="size-3" /> Run
            </Button>
          ) : null}
        </div>
      </div>

      {/* Editor */}
      <div className="bg-card">
        <CodeMirror
          value={editing ? draft : initialSql}
          extensions={extensions as never}
          theme={theme as never}
          editable={editing}
          readOnly={!editing}
          basicSetup={{
            lineNumbers: showLineNumbers,
            foldGutter: false,
            highlightActiveLine: editing,
            highlightActiveLineGutter: editing,
            autocompletion: editing,
            indentOnInput: editing,
            bracketMatching: true,
            closeBrackets: editing,
          }}
          onChange={(value) => setDraft(value)}
          className="text-sm"
        />
      </div>
    </div>
  );
}
