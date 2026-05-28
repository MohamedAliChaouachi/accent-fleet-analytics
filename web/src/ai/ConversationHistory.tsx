import { useMemo, useState } from "react";
import {
  MessageSquare,
  Plus,
  Search,
  Trash2,
} from "lucide-react";
import { Button } from "@/components/ui/Button";
import { Input } from "@/components/ui/Input";
import { cn } from "@/lib/cn";

export interface ConversationSummary {
  id: string;
  title: string;
  updatedAt: number;
  messageCount: number;
}

interface ConversationHistoryProps {
  conversations: ReadonlyArray<ConversationSummary>;
  activeId: string | null;
  onSelect: (id: string) => void;
  onNew: () => void;
  onDelete: (id: string) => void;
  /** Compact width for the slide-out (no search bar). */
  compact?: boolean;
  className?: string;
}

// Right-rail recent conversations list. In compact mode (slide-out
// panel) the search bar collapses to keep vertical space for messages.
export function ConversationHistory({
  conversations,
  activeId,
  onSelect,
  onNew,
  onDelete,
  compact = false,
  className,
}: ConversationHistoryProps) {
  const [filter, setFilter] = useState("");

  const filtered = useMemo(() => {
    if (!filter.trim()) return conversations;
    const needle = filter.trim().toLowerCase();
    return conversations.filter((c) => c.title.toLowerCase().includes(needle));
  }, [conversations, filter]);

  return (
    <div className={cn("flex h-full min-h-0 flex-col gap-3", className)}>
      <div className="flex items-center justify-between gap-2">
        <p className="text-2xs font-semibold uppercase tracking-widest text-muted-foreground">
          History
        </p>
        <Button
          variant="ghost"
          size="icon-sm"
          onClick={onNew}
          aria-label="New conversation"
          title="New conversation"
        >
          <Plus className="size-3.5" />
        </Button>
      </div>

      {!compact ? (
        <Input
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder="Search history…"
          leadingIcon={<Search />}
          className="h-8 text-xs"
        />
      ) : null}

      <ul className="flex-1 min-h-0 space-y-0.5 overflow-y-auto pr-1">
        {filtered.length === 0 ? (
          <li className="px-2 py-6 text-center text-xs text-muted-foreground">
            {conversations.length === 0
              ? "Your past chats will show up here."
              : "No matches."}
          </li>
        ) : (
          filtered.map((c) => (
            <ConvoRow
              key={c.id}
              convo={c}
              active={c.id === activeId}
              onClick={() => onSelect(c.id)}
              onDelete={() => onDelete(c.id)}
            />
          ))
        )}
      </ul>
    </div>
  );
}

function ConvoRow({
  convo,
  active,
  onClick,
  onDelete,
}: {
  convo: ConversationSummary;
  active: boolean;
  onClick: () => void;
  onDelete: () => void;
}) {
  return (
    <li>
      <div
        role="button"
        tabIndex={0}
        onClick={onClick}
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            onClick();
          }
        }}
        className={cn(
          "group/h flex cursor-pointer items-start gap-2 rounded-md px-2 py-2 text-sm transition-colors",
          "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring",
          active
            ? "bg-ai/10 text-foreground"
            : "text-muted-foreground hover:bg-secondary hover:text-foreground",
        )}
      >
        <MessageSquare
          className={cn(
            "mt-0.5 size-3.5 shrink-0",
            active && "text-ai",
          )}
        />
        <div className="min-w-0 flex-1">
          <p className="truncate text-xs leading-snug">{convo.title}</p>
          <p className="mt-0.5 text-2xs text-muted-foreground/80">
            {formatRelative(convo.updatedAt)} · {convo.messageCount} msg
          </p>
        </div>
        <button
          type="button"
          onClick={(e) => {
            e.stopPropagation();
            onDelete();
          }}
          className="invisible mt-0.5 rounded p-1 text-muted-foreground hover:bg-destructive/10 hover:text-destructive group-hover/h:visible"
          aria-label="Delete conversation"
          title="Delete"
        >
          <Trash2 className="size-3" />
        </button>
      </div>
    </li>
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
