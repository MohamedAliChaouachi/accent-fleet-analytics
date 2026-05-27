// Shared body for ChatPanel (slide-out) and FullPageAssistant.
//
// Both surfaces need the same composition — transcript on top, composer
// at the bottom, optional suggestions when empty, optional sidebar with
// recent conversations. The only thing that differs is layout chrome:
// the panel collapses the history into a top drawer; the page renders
// it as a fixed right rail.
//
// State comes from the parent via the `chat` prop (a `useAIChat()`
// return) so both shells share a single source of truth. That matters
// because the FAB-launched panel and the /ai page point to the same
// localStorage-backed history — opening one and then the other should
// show identical conversations.

import { useEffect, useMemo, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { listTenants } from "@/api/admin";
import { useAuth } from "@/auth/AuthContext";
import { Bot, Loader2, MessageSquare } from "lucide-react";
import { cn } from "@/lib/cn";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/Select";
import { ChatComposer, type ChatComposerHandle } from "./ChatComposer";
import { ConversationHistory } from "./ConversationHistory";
import { MessageList } from "./MessageList";
import { QuerySuggestions } from "./QuerySuggestions";
import type { UseAIChatReturn } from "./useAIChat";

interface AssistantBodyProps {
  chat: UseAIChatReturn;
  /** Compact layout for the slide-out panel. */
  compact?: boolean;
  /** Route used to pick context-aware suggestions. */
  pathname?: string;
  /** Whether the user is a superadmin (drives tenant picker). */
  isSuperadmin: boolean;
  className?: string;
}

export function AssistantBody({
  chat,
  compact = false,
  pathname,
  isSuperadmin,
  className,
}: AssistantBodyProps) {
  const composerRef = useRef<ChatComposerHandle | null>(null);
  const [input, setInput] = useState("");

  // Focus the composer when the body mounts or the active conversation
  // changes. This is the "instantly typeable" behaviour the brief asks
  // for: opening the assistant should put the cursor in the input.
  useEffect(() => {
    composerRef.current?.focus();
  }, [chat.activeId]);

  function handleSend() {
    const q = input.trim();
    if (!q) return;
    chat.send(q);
    setInput("");
  }

  function handlePick(prompt: string) {
    // Set the composer value, focus, and let the user review/edit
    // before submitting. Auto-send would be too aggressive — sometimes
    // users want to tweak the prompt first.
    composerRef.current?.setValue(prompt);
    setInput(prompt);
  }

  const empty = chat.messages.length === 0 && !chat.isLoading;

  return (
    <div className={cn("flex h-full min-h-0 flex-col", className)}>
      {/* Header strip — tenant picker (superadmin only). */}
      {isSuperadmin ? (
        <TenantStrip
          value={chat.tenant}
          onChange={chat.setTenant}
          compact={compact}
        />
      ) : null}

      {/* Body: messages + (in full mode) right rail with history. */}
      <div className="flex min-h-0 flex-1">
        <div className="flex min-h-0 min-w-0 flex-1 flex-col">
          {empty ? (
            <EmptyState
              compact={compact}
              pathname={pathname}
              onPick={handlePick}
            />
          ) : (
            <MessageList
              messages={chat.messages}
              loading={chat.isLoading}
              error={chat.error}
              compact={compact}
              feedbackByEventId={chat.feedbackByEventId}
              onFeedbackChange={chat.setFeedback}
            />
          )}

          <div
            className={cn(
              "border-t border-border bg-background/80 backdrop-blur",
              compact ? "px-3 py-2" : "px-4 py-3",
            )}
          >
            <ChatComposer
              ref={composerRef}
              value={input}
              onChange={setInput}
              onSubmit={handleSend}
              onCancel={chat.isLoading ? chat.cancel : undefined}
              loading={chat.isLoading}
              compact={compact}
              placeholder={
                isSuperadmin && !chat.tenant
                  ? "Pick a tenant above, then ask a question…"
                  : "Ask anything about your fleet…"
              }
            />
          </div>
        </div>

        {/* Right rail — full layout only. The compact panel renders the
            history elsewhere (as a collapsible "Recents" header). */}
        {!compact ? (
          <aside className="hidden w-64 shrink-0 flex-col border-l border-border bg-card/40 p-3 lg:flex">
            <ConversationHistory
              conversations={chat.conversations.map(toSummary)}
              activeId={chat.activeId}
              onSelect={chat.switchTo}
              onNew={chat.newChat}
              onDelete={chat.deleteConvo}
            />
          </aside>
        ) : null}
      </div>
    </div>
  );
}

function toSummary(c: UseAIChatReturn["conversations"][number]) {
  return {
    id: c.id,
    title: c.title,
    updatedAt: c.updatedAt,
    messageCount: c.messages.length,
  };
}

// ---------------------------------------------------------------------------
// Tenant strip
// ---------------------------------------------------------------------------

function TenantStrip({
  value,
  onChange,
  compact,
}: {
  value: string;
  onChange: (v: string) => void;
  compact: boolean;
}) {
  // Tenants list lives in the same React Query cache as the TopBar
  // dropdown — they share the `["tenants"]` key so we make a single
  // request per session.
  const tenantsQ = useQuery({
    queryKey: ["tenants"],
    queryFn: listTenants,
    staleTime: 5 * 60_000,
  });

  return (
    <div
      className={cn(
        "flex items-center gap-2 border-b border-border bg-muted/30",
        compact ? "px-3 py-2" : "px-4 py-2",
      )}
    >
      <span className="text-2xs font-semibold uppercase tracking-widest text-muted-foreground">
        Tenant
      </span>
      <Select value={value || undefined} onValueChange={onChange}>
        <SelectTrigger className="h-7 max-w-[16rem] text-xs">
          {tenantsQ.isLoading ? (
            <span className="flex items-center gap-1 text-muted-foreground">
              <Loader2 className="size-3 animate-spin" />
              Loading…
            </span>
          ) : (
            <SelectValue placeholder="Select a tenant…" />
          )}
        </SelectTrigger>
        <SelectContent>
          {(tenantsQ.data ?? []).map((t) => (
            <SelectItem key={t.tenant_id} value={String(t.tenant_id)}>
              {t.display_name} (#{t.tenant_id})
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Empty state — greeting + suggestion chips
// ---------------------------------------------------------------------------

function EmptyState({
  compact,
  pathname,
  onPick,
}: {
  compact: boolean;
  pathname?: string;
  onPick: (q: string) => void;
}) {
  const { user } = useAuth();
  const greeting = useMemo(() => buildGreeting(user?.email), [user?.email]);

  return (
    <div
      className={cn(
        "flex flex-1 min-h-0 flex-col items-center justify-center text-center",
        compact ? "px-4 py-6" : "px-6 py-10",
      )}
    >
      <div className="flex size-12 items-center justify-center rounded-2xl bg-gradient-ai text-white shadow-ai-glow/40">
        <Bot className="size-6" />
      </div>
      <h2
        className={cn(
          "mt-4 font-semibold tracking-tight text-foreground",
          compact ? "text-lg" : "text-2xl",
        )}
      >
        {greeting}
      </h2>
      <p
        className={cn(
          "mt-2 max-w-md text-muted-foreground",
          compact ? "text-xs" : "text-sm",
        )}
      >
        Ask a natural-language question about trips, vehicles, risk, or
        maintenance. I&apos;ll write and run the SQL.
      </p>
      <div className="mt-6 flex w-full max-w-2xl items-center gap-2 text-2xs uppercase tracking-widest text-muted-foreground">
        <span className="h-px flex-1 bg-border" />
        <MessageSquare className="size-3" /> Suggested for this page
        <span className="h-px flex-1 bg-border" />
      </div>
      <div className="mt-4">
        <QuerySuggestions pathname={pathname} onPick={onPick} />
      </div>
    </div>
  );
}

function buildGreeting(email: string | undefined): string {
  const hour = new Date().getHours();
  const timeOfDay =
    hour < 5
      ? "Late night"
      : hour < 12
        ? "Morning"
        : hour < 18
          ? "Afternoon"
          : "Evening";
  const name = email ? email.split("@")[0] : "there";
  const pretty = name.charAt(0).toUpperCase() + name.slice(1);
  return `${timeOfDay}, ${pretty}`;
}
