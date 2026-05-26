// AI chat page — conversational Text2SQL with persistent history.
//
// Three things make this file longer than a typical page:
//
//   1. Multi-conversation model. We persist N chats per user, not just
//      the most-recent one. Storage shape is `{ v: 2, conversations:
//      [{ id, title, messages, tenant, updatedAt }], activeId }` keyed
//      by user email. Bumping the version invalidates the old v1
//      single-chat layout silently — users had nothing important
//      there anyway.
//
//   2. Server still sees only the active conversation's last 6 turns.
//      Chosen conversations don't bleed into each other. Switching
//      chats is purely a client-side state swap.
//
//   3. Errors are NOT persisted. They're transient UI ("rate limit hit,
//      try again") and a stale error from yesterday isn't useful to
//      bring back. We render the current attempt's error inline but
//      drop it on save.
//
// Layout: message stream + composer on the left, 240px sidebar
// (Recents list) on the right. Matches the requested placement —
// the global app nav already occupies the left edge, so putting the
// chat history on the right keeps the two from competing.

import { useMutation, useQuery } from "@tanstack/react-query";
import {
  useEffect,
  useMemo,
  useRef,
  useState,
  type FormEvent,
  type KeyboardEvent,
} from "react";

import {
  askAI,
  AIQueryError,
  MAX_HISTORY_TURNS,
  type AIQueryResponse,
  type ChatTurn,
} from "@/api/ai";
import { listTenants } from "@/api/admin";
import { useAuth } from "@/auth/AuthContext";
import { StateMessage } from "@/components/StateMessage";
import { BarChart } from "@/components/charts/BarChart";
import { LineChart } from "@/components/charts/LineChart";
import { PieChart } from "@/components/charts/PieChart";

// Bumping this string is the easiest way to tell, from the browser
// console, whether a fresh build actually shipped. If you change the
// page and don't see the new banner, your bundle is cached.
const BUILD_TAG = "AIChat v4.0 — fix send() convoId race";
if (typeof window !== "undefined") {
  // eslint-disable-next-line no-console
  console.info(`[${BUILD_TAG}] loaded`);
}

const EXAMPLE_QUESTIONS: ReadonlyArray<string> = [
  "Show monthly trip volume for the past 12 months.",
  "Top 5 vehicles by total cost last month.",
  "What's the distribution of risk categories across the fleet?",
  "How many devices are in each behaviour cluster?",
];

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** What lives on screen. Errors are in-memory only. */
type DisplayMessage =
  | { id: number; role: "user"; content: string }
  | { id: number; role: "assistant"; response: AIQueryResponse };

/** What we persist — same as DisplayMessage today (errors are excluded). */
type PersistedMessage = DisplayMessage;

interface Conversation {
  id: string;
  title: string;
  messages: PersistedMessage[];
  tenant: string;
  updatedAt: number; // epoch ms — drives the Recents sort order
  nextMsgId: number; // monotonic so React keys never collide
}

interface PersistedStateV2 {
  v: 2;
  conversations: Conversation[];
  activeId: string | null;
}

// ---------------------------------------------------------------------------
// Persistence (per-user, localStorage)
// ---------------------------------------------------------------------------

const STORAGE_VERSION = 2;
const STORAGE_PREFIX = "accent.ai.chats.v2";

function storageKeyFor(email: string | undefined): string | null {
  if (!email) return null;
  return `${STORAGE_PREFIX}:${email.toLowerCase()}`;
}

function loadConvos(email: string | undefined): PersistedStateV2 | null {
  const key = storageKeyFor(email);
  if (!key) return null;
  try {
    const raw = window.localStorage.getItem(key);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as PersistedStateV2;
    if (parsed.v !== STORAGE_VERSION) return null;
    if (!Array.isArray(parsed.conversations)) return null;
    return parsed;
  } catch {
    return null;
  }
}

function saveConvos(
  email: string | undefined,
  conversations: ReadonlyArray<Conversation>,
  activeId: string | null,
): void {
  const key = storageKeyFor(email);
  if (!key) return;
  try {
    const body: PersistedStateV2 = {
      v: STORAGE_VERSION,
      conversations: conversations as Conversation[],
      activeId,
    };
    window.localStorage.setItem(key, JSON.stringify(body));
  } catch {
    // Quota exceeded — silently drop. The in-memory state remains
    // correct for the current session. Realistic recovery would prune
    // the oldest conversation; not worth the code for now.
  }
}

function clearConvos(email: string | undefined): void {
  const key = storageKeyFor(email);
  if (!key) return;
  try {
    window.localStorage.removeItem(key);
  } catch {
    /* ignore */
  }
}

// Sidebar visibility is a per-browser preference, not per-user — same
// person on the same machine wants the same layout regardless of which
// account they sign in with. Hence a single global key rather than the
// email-scoped one above.
const SIDEBAR_PREF_KEY = "accent.ai.sidebar.v1";

function loadSidebarOpen(): boolean {
  try {
    const raw = window.localStorage.getItem(SIDEBAR_PREF_KEY);
    if (raw === null) return true; // default: visible
    return raw === "1";
  } catch {
    return true;
  }
}

function saveSidebarOpen(open: boolean): void {
  try {
    window.localStorage.setItem(SIDEBAR_PREF_KEY, open ? "1" : "0");
  } catch {
    /* ignore */
  }
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

function newConvoId(): string {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return crypto.randomUUID();
  }
  return `c_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

function newConversation(tenant: string): Conversation {
  return {
    id: newConvoId(),
    title: "New conversation",
    messages: [],
    tenant,
    updatedAt: Date.now(),
    nextMsgId: 1,
  };
}

function deriveTitle(text: string): string {
  const cleaned = text.trim().replace(/\s+/g, " ");
  if (cleaned.length <= 50) return cleaned;
  return cleaned.slice(0, 50).trimEnd() + "…";
}

function formatRelativeTime(ts: number): string {
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

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export function AIChat() {
  const { user } = useAuth();
  const isSuperadmin = user?.role === "superadmin";

  // Load tenants once per session; the list rarely changes during a
  // single sitting. `enabled` gates the request on the role so a
  // tenant_user never hits the superadmin-only endpoint and gets a 403
  // in DevTools that they can't act on.
  const tenantsQ = useQuery({
    queryKey: ["tenants"],
    queryFn: listTenants,
    enabled: isSuperadmin,
    staleTime: 5 * 60_000,
  });

  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [activeId, setActiveId] = useState<string | null>(null);
  const [input, setInput] = useState("");
  // Errors are kept out of the persisted conversation. They show under
  // the message list for the current attempt and are cleared on
  // conversation switch or next successful send.
  const [transientError, setTransientError] = useState<Error | null>(null);
  // Gate save on hydrate so first render doesn't overwrite stored data.
  const [hydrated, setHydrated] = useState(false);

  // Sidebar visibility (toggle button). Initial render uses the default
  // so SSR/initial-paint matches; an effect below pulls the saved
  // preference on mount.
  const [sidebarOpen, setSidebarOpen] = useState(true);
  useEffect(() => {
    setSidebarOpen(loadSidebarOpen());
  }, []);

  function toggleSidebar() {
    setSidebarOpen((v) => {
      const next = !v;
      saveSidebarOpen(next);
      return next;
    });
  }

  const scrollerRef = useRef<HTMLDivElement | null>(null);

  // Derived active conversation + its visible fields.
  const active = useMemo(
    () => conversations.find((c) => c.id === activeId) ?? null,
    [conversations, activeId],
  );
  const messages: ReadonlyArray<DisplayMessage> = active?.messages ?? [];
  const tenant = active?.tenant ?? "";

  // Hydrate from localStorage once the user identity is known.
  useEffect(() => {
    if (!user?.email) {
      setConversations([]);
      setActiveId(null);
      setHydrated(true);
      return;
    }
    const saved = loadConvos(user.email);
    if (saved && saved.conversations.length > 0) {
      // Sort by updatedAt desc so the Recents list reads top-to-bottom
      // newest-first the moment we render.
      const sorted = [...saved.conversations].sort(
        (a, b) => b.updatedAt - a.updatedAt,
      );
      setConversations(sorted);
      // Prefer the saved activeId if it still exists; otherwise pick
      // the most recently updated convo.
      const stillExists =
        saved.activeId && sorted.some((c) => c.id === saved.activeId);
      setActiveId(stillExists ? saved.activeId : sorted[0]!.id);
    } else {
      setConversations([]);
      setActiveId(null);
    }
    setHydrated(true);
  }, [user?.email]);

  // Persist on every meaningful change after hydration. We refuse to
  // write an empty conversation list — both as a defensive measure
  // against the race between hydrate and this effect re-firing on
  // user?.email change, and to keep an empty in-memory state from
  // wiping a stored one. Explicit deletion is what calls clearConvos.
  useEffect(() => {
    if (!hydrated) return;
    if (conversations.length === 0) return;
    saveConvos(user?.email, conversations, activeId);
  }, [hydrated, user?.email, conversations, activeId]);

  // Auto-scroll the message stream when the active conversation grows.
  useEffect(() => {
    const el = scrollerRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [messages.length, activeId]);

  // -------------------------------------------------------------------------
  // Mutations & actions
  // -------------------------------------------------------------------------

  const mutation = useMutation<
    AIQueryResponse,
    Error,
    { convoId: string; question: string }
  >({
    mutationFn: async ({ question }) => {
      const tenantId =
        isSuperadmin && tenant.trim() ? Number(tenant.trim()) : undefined;
      const history: ChatTurn[] = messages
        .flatMap<ChatTurn>((m) => {
          if (m.role === "user") return [{ role: "user", content: m.content }];
          if (m.role === "assistant")
            return [{ role: "assistant", content: m.response.summary }];
          return [];
        })
        .slice(-MAX_HISTORY_TURNS);
      return askAI({ question, tenant_id: tenantId, history });
    },
    onSuccess: (resp, vars) => {
      // Append assistant reply to the same conversation that asked.
      // Going through `vars.convoId` (captured at send time) instead
      // of `activeId` avoids a race if the user switches chats while
      // the request is in flight.
      setConversations((prev) =>
        prev.map((c) =>
          c.id !== vars.convoId
            ? c
            : {
                ...c,
                messages: [
                  ...c.messages,
                  { id: c.nextMsgId, role: "assistant", response: resp },
                ],
                nextMsgId: c.nextMsgId + 1,
                updatedAt: Date.now(),
              },
        ),
      );
      setTransientError(null);
    },
    onError: (err) => setTransientError(err),
  });

  function send() {
    const q = input.trim();
    if (!q || mutation.isPending) return;
    // Tenant guard is SOFT — we don't disable the Send button on
    // missing tenant (disabled styling washed out and read as "the
    // button disappeared"). Instead, surface an inline error so the
    // user gets immediate feedback and can fix it.
    if (isSuperadmin && !tenant.trim()) {
      setTransientError(
        new Error("Pick a tenant from the dropdown before sending."),
      );
      return;
    }

    // Resolve / allocate the target conversation BEFORE touching React
    // state. Two reasons this can't live inside the setConversations
    // updater:
    //
    //   * React batches setState calls in event handlers — the updater
    //     function runs during the NEXT render, not synchronously. So
    //     if we mutated an outer `convoId` from inside the updater, the
    //     `mutation.mutate({ convoId })` call below would fire with the
    //     stale pre-call value (typically null for a fresh chat), and
    //     onSuccess would do `prev.map(c => c.id !== null ? c : …)` —
    //     no row matches, the assistant reply is silently dropped, and
    //     the UI shows the user bubble with nothing after it. That was
    //     the v3.x bug.
    //
    //   * State updaters must be pure. StrictMode invokes them twice
    //     in dev to detect impurity; a `convoId = fresh.id` side effect
    //     inside the updater would generate two different UUIDs across
    //     the two invocations and leak orphan conversations.
    //
    // Compute everything synchronously here, then pass the resolved
    // id into both the state update and the mutation.
    const existing = activeId
      ? conversations.find((c) => c.id === activeId) ?? null
      : null;
    const conversation = existing ?? newConversation(isSuperadmin ? tenant : "");
    const convoId = conversation.id;
    const isNew = existing === null;

    setConversations((prev) => {
      // Re-check inside the updater in case StrictMode replays it with
      // a state that already contains the conversation we just created
      // (defensive: `prev` should equal the snapshot we read above,
      // but cheap to verify and keeps the updater idempotent).
      const working =
        isNew && !prev.some((c) => c.id === convoId)
          ? [conversation, ...prev]
          : prev;
      return working.map((c) => {
        if (c.id !== convoId) return c;
        const isFirstUserMsg = c.messages.every((m) => m.role !== "user");
        return {
          ...c,
          title: isFirstUserMsg ? deriveTitle(q) : c.title,
          messages: [...c.messages, { id: c.nextMsgId, role: "user", content: q }],
          nextMsgId: c.nextMsgId + 1,
          updatedAt: Date.now(),
        };
      });
    });
    if (activeId !== convoId) setActiveId(convoId);
    setInput("");
    setTransientError(null);
    mutation.mutate({ convoId, question: q });
  }

  function onSubmit(e: FormEvent) {
    e.preventDefault();
    send();
  }

  function onKeyDown(e: KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      send();
    }
  }

  function newChat() {
    // Don't allocate a brand-new convo here — just deselect. Sending
    // the first message will auto-create one. This avoids littering
    // the sidebar with empty "New conversation" entries.
    setActiveId(null);
    setInput("");
    setTransientError(null);
    mutation.reset();
  }

  function switchTo(id: string) {
    if (id === activeId) return;
    setActiveId(id);
    setInput("");
    setTransientError(null);
    mutation.reset();
  }

  function deleteConvo(id: string) {
    setConversations((prev) => {
      const remaining = prev.filter((c) => c.id !== id);
      // If we just deleted the active one, jump to the next most
      // recently updated convo (or to empty state if none left).
      if (id === activeId) {
        if (remaining.length === 0) {
          setActiveId(null);
          // The persist effect skips empty lists, so we explicitly
          // wipe the saved data when the user empties their history.
          clearConvos(user?.email);
        } else {
          const sorted = [...remaining].sort(
            (a, b) => b.updatedAt - a.updatedAt,
          );
          setActiveId(sorted[0]!.id);
        }
      }
      return remaining;
    });
  }

  function updateTenant(value: string) {
    if (!active) {
      // No active convo yet — stash the tenant on a fresh one so
      // `send()` finds it. Cheaper than juggling a parallel state.
      const fresh = newConversation(value);
      setConversations((prev) => [fresh, ...prev]);
      setActiveId(fresh.id);
      return;
    }
    setConversations((prev) =>
      prev.map((c) => (c.id === active.id ? { ...c, tenant: value } : c)),
    );
  }

  // -------------------------------------------------------------------------
  // Render
  // -------------------------------------------------------------------------

  const showEmpty = !active || (active.messages.length === 0 && !mutation.isPending);
  const greeting = useMemo(() => buildGreeting(user?.email), [user?.email]);
  const sortedConvos = useMemo(
    () => [...conversations].sort((a, b) => b.updatedAt - a.updatedAt),
    [conversations],
  );
  // Only disable on the two unambiguous cases: a request is in flight,
  // or the textarea is empty. Tenant selection is checked in `send()`
  // and surfaces as an inline message — that keeps the Send button
  // visually constant so it never looks like it vanished.
  const sendDisabled = mutation.isPending || !input.trim();

  return (
    <div className="flex h-[calc(100vh-7rem)] gap-3">
      {/* --------- Main: chat ---------------------------------------------- */}
      <div className="flex min-w-0 flex-1 flex-col">
        {/* Top bar: tenant input + small "New chat" for mobile (sidebar hidden). */}
        <div className="flex items-center justify-between border-b border-slate-200 px-2 pb-3">
          <div className="flex items-center gap-3">
            {isSuperadmin ? (
              <label className="flex items-center gap-2 text-xs text-slate-500">
                <span className="uppercase tracking-wider">Tenant</span>
                <select
                  value={tenant}
                  onChange={(e) => updateTenant(e.target.value)}
                  className="min-w-[10rem] rounded-md border border-slate-300 bg-white px-2 py-1 text-sm text-slate-800"
                  disabled={tenantsQ.isLoading}
                >
                  {/* Disabled placeholder when nothing's picked yet.
                      "All tenants" used to live here but ai_query
                      requires a single tenant bind (see "Phase 2"
                      comment in app/ai/routers/ai_query.py), so we
                      force an explicit choice instead. */}
                  <option value="" disabled>
                    {tenantsQ.isLoading ? "Loading…" : "Select a tenant…"}
                  </option>
                  {(tenantsQ.data ?? []).map((t) => (
                    <option key={t.tenant_id} value={String(t.tenant_id)}>
                      {t.display_name} (#{t.tenant_id})
                    </option>
                  ))}
                </select>
              </label>
            ) : null}
          </div>
          <div className="flex items-center gap-2">
            {/* Always-visible "+ New chat". Lives in the top bar (not
                in the Recents sidebar) so it stays reachable even
                when the sidebar is collapsed or hidden on mobile. */}
            <button
              type="button"
              onClick={newChat}
              className="inline-flex items-center gap-1 rounded-md border border-slate-300 bg-white px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50"
              title="Start a new chat"
            >
              <span aria-hidden="true">+</span>
              <span>New chat</span>
            </button>
            {/* Desktop-only: collapse the Recents panel on the right. */}
            <button
              type="button"
              onClick={toggleSidebar}
              className="hidden items-center gap-1.5 rounded-md border border-slate-300 bg-white px-3 py-1 text-xs text-slate-600 hover:bg-slate-50 md:inline-flex"
              title={sidebarOpen ? "Hide Recents" : "Show Recents"}
              aria-pressed={sidebarOpen}
            >
              <span aria-hidden="true">{sidebarOpen ? "›|" : "|‹"}</span>
              <span>{sidebarOpen ? "Hide Recents" : "Show Recents"}</span>
            </button>
          </div>
        </div>

        {/* Conversation stream */}
        <div ref={scrollerRef} className="flex-1 overflow-y-auto px-2 py-6">
          {showEmpty ? (
            <EmptyState
              greeting={greeting}
              onPick={(q) => setInput(q)}
              superadminMissingTenant={isSuperadmin && !tenant.trim()}
            />
          ) : (
            <div className="mx-auto flex max-w-3xl flex-col gap-6">
              {messages.map((m) => (
                <MessageBubble key={m.id} message={m} />
              ))}
              {mutation.isPending ? <TypingIndicator /> : null}
            </div>
          )}
          {/* Errors render OUTSIDE the showEmpty branch on purpose:
              a click on Send with "All tenants" selected sets a
              transient error but adds no message, so messages.length
              stays 0 and showEmpty is true. If we left this inside the
              else branch the error would never appear and the button
              would look like it did nothing. */}
          {transientError ? (
            <div className="mx-auto mt-6 flex max-w-3xl">
              <ErrorBubble error={transientError} />
            </div>
          ) : null}
        </div>

        {/* Composer */}
        <form
          onSubmit={onSubmit}
          className="mx-auto w-full max-w-3xl px-2 pt-3"
        >
          <div className="flex items-end gap-2 rounded-2xl border border-slate-300 bg-white p-2 shadow-sm focus-within:border-brand">
            <textarea
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={onKeyDown}
              rows={1}
              placeholder="Ask anything about your fleet…"
              className="block max-h-40 min-h-[2.5rem] flex-1 resize-none bg-transparent px-2 py-2 text-sm text-slate-800 placeholder:text-slate-400 focus:outline-none"
              disabled={mutation.isPending}
            />
            <button
              type="submit"
              disabled={sendDisabled}
              // Disabled palette pinned to slate-400 (medium grey) with
              // white text + opacity-70 so contrast is unmistakable.
              // `min-w-[5rem]` guarantees the button keeps its footprint
              // no matter how long the textarea placeholder grows —
              // earlier "Send disappears on 'All tenants'" reports
              // looked like the disabled bg was washing out against
              // the white composer. shrink-0 keeps flex from squashing
              // it on narrow widths.
              className="min-w-[5rem] shrink-0 rounded-xl bg-brand px-4 py-2 text-sm font-medium text-white shadow-sm transition-colors hover:bg-brand-dark disabled:cursor-not-allowed disabled:bg-slate-400 disabled:text-white disabled:opacity-70 disabled:shadow-none"
            >
              {mutation.isPending ? "…" : "Send"}
            </button>
          </div>
          <p className="mt-2 text-center text-[11px] text-slate-400">
            Enter to send · Shift+Enter for a new line · scoped to your tenant
          </p>
        </form>
      </div>

      {/* --------- Sidebar: Recents (right side) --------------------------- */}
      {/* `hidden ... md:flex` keeps the panel off mobile entirely; the
          extra `sidebarOpen` gate lets desktop users collapse it too. */}
      {sidebarOpen ? (
      <aside className="hidden w-60 shrink-0 flex-col border-l border-slate-200 pl-3 md:flex">
        <div className="flex items-center justify-between pb-2">
          <span className="text-xs font-medium uppercase tracking-wider text-slate-500">
            Recents
          </span>
          {/* "+ New chat" lives in the top bar now, not here. The only
              control left in the sidebar header is the close button. */}
          <button
            type="button"
            onClick={toggleSidebar}
            className="rounded-md px-1.5 py-1 text-slate-400 hover:bg-slate-100 hover:text-slate-700"
            title="Hide Recents"
            aria-label="Hide Recents"
          >
            ×
          </button>
        </div>
        {sortedConvos.length === 0 ? (
          <p className="px-1 pt-2 text-xs text-slate-400">
            Your past chats will show up here.
          </p>
        ) : (
          <ul className="flex-1 space-y-1 overflow-y-auto">
            {sortedConvos.map((c) => (
              <li key={c.id}>
                <ConvoItem
                  convo={c}
                  active={c.id === activeId}
                  onClick={() => switchTo(c.id)}
                  onDelete={() => deleteConvo(c.id)}
                />
              </li>
            ))}
          </ul>
        )}
      </aside>
      ) : null}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Sidebar item
// ---------------------------------------------------------------------------

function ConvoItem({
  convo,
  active,
  onClick,
  onDelete,
}: {
  convo: Conversation;
  active: boolean;
  onClick: () => void;
  onDelete: () => void;
}) {
  return (
    <div
      className={`group flex cursor-pointer items-center justify-between gap-1 rounded-md px-2 py-2 text-sm transition-colors ${
        active
          ? "bg-brand/10 text-brand-dark"
          : "text-slate-700 hover:bg-slate-100"
      }`}
      onClick={onClick}
      role="button"
      tabIndex={0}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          onClick();
        }
      }}
    >
      <div className="min-w-0 flex-1">
        <p className="truncate text-[13px] leading-tight">{convo.title}</p>
        <p className="mt-0.5 text-[10px] uppercase tracking-wider text-slate-400">
          {formatRelativeTime(convo.updatedAt)}
        </p>
      </div>
      <button
        type="button"
        onClick={(e) => {
          e.stopPropagation();
          onDelete();
        }}
        className="invisible rounded-md px-1 text-xs text-slate-400 hover:text-rose-500 group-hover:visible"
        title="Delete this conversation"
        aria-label="Delete conversation"
      >
        ×
      </button>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Empty / landing state
// ---------------------------------------------------------------------------

function EmptyState({
  greeting,
  onPick,
  superadminMissingTenant,
}: {
  greeting: string;
  onPick: (q: string) => void;
  superadminMissingTenant: boolean;
}) {
  return (
    <div className="mx-auto flex max-w-2xl flex-col items-center pt-16 text-center">
      <h1 className="text-3xl font-semibold tracking-tight text-slate-800">
        {greeting}
      </h1>
      <p className="mt-3 text-sm text-slate-500">
        Ask a natural-language question about trips, vehicles, risk, or
        maintenance.
      </p>
      {superadminMissingTenant ? (
        <p className="mt-2 text-xs text-amber-600">
          Pick a tenant in the top bar to start a conversation.
        </p>
      ) : null}
      <div className="mt-8 flex w-full flex-wrap justify-center gap-2">
        {EXAMPLE_QUESTIONS.map((q) => (
          <button
            key={q}
            type="button"
            onClick={() => onPick(q)}
            className="rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs text-slate-600 shadow-sm hover:bg-slate-50"
          >
            {q}
          </button>
        ))}
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

// ---------------------------------------------------------------------------
// Message bubbles
// ---------------------------------------------------------------------------

function MessageBubble({ message }: { message: DisplayMessage }) {
  if (message.role === "user") {
    return (
      <div className="flex justify-end">
        <div className="max-w-[80%] whitespace-pre-wrap rounded-2xl rounded-br-sm bg-brand px-4 py-2 text-sm text-white shadow-sm">
          {message.content}
        </div>
      </div>
    );
  }
  return (
    <div className="flex justify-start">
      <div className="max-w-[92%] rounded-2xl rounded-bl-sm border border-slate-200 bg-white px-4 py-3 shadow-sm">
        <p className="text-sm leading-relaxed text-slate-800">
          {message.response.summary}
        </p>
        <AssistantChart response={message.response} />
        <p className="mt-2 text-[11px] text-slate-400">
          {message.response.row_count}{" "}
          {message.response.row_count === 1 ? "row" : "rows"} ·{" "}
          {message.response.elapsed_ms} ms · {message.response.provider}/
          {message.response.model}
        </p>
      </div>
    </div>
  );
}

function TypingIndicator() {
  return (
    <div className="flex justify-start">
      <div className="rounded-2xl rounded-bl-sm border border-slate-200 bg-white px-4 py-3 shadow-sm">
        <span className="inline-flex items-center gap-1 text-sm text-slate-400">
          <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-slate-400" />
          <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-slate-400 [animation-delay:150ms]" />
          <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-slate-400 [animation-delay:300ms]" />
        </span>
      </div>
    </div>
  );
}

function ErrorBubble({ error }: { error: Error }) {
  return (
    <div className="flex justify-start">
      <div className="max-w-[92%] rounded-2xl rounded-bl-sm border border-rose-200 bg-rose-50 px-4 py-3 shadow-sm">
        {error instanceof AIQueryError ? (
          <StateMessage tone="error">
            <strong className="block">{prettyStage(error.stage)}</strong>
            <span className="mt-1 block whitespace-pre-wrap text-xs text-slate-700">
              {error.message}
            </span>
            <span className="mt-2 block text-[11px] text-slate-500">
              {stageHint(error.stage)}
            </span>
          </StateMessage>
        ) : (
          <StateMessage tone="error">{error.message}</StateMessage>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Chart rendering
// ---------------------------------------------------------------------------

function AssistantChart({ response }: { response: AIQueryResponse }) {
  const { chart_type, rows, columns } = response;
  if (rows.length === 0) return null;

  // chart_type="table" or single-column results: render the rows
  // themselves. Without this the bubble shows just the summary sentence
  // and the row-count footer — which reads as "the bot didn't show me
  // anything" for questions like "top 5 vehicles by cost". The same
  // visual treatment doubles as the empty-state for low-cardinality
  // results that the chart suggester refused to plot.
  if (chart_type === "table" || columns.length < 2) {
    return (
      <div className="mt-3 overflow-x-auto rounded-md border border-slate-100 bg-slate-50/60 p-3">
        <ResultTable rows={rows} columns={columns} />
      </div>
    );
  }

  const [xKey, yKey] = columns;

  return (
    <div className="mt-3 rounded-md border border-slate-100 bg-slate-50/60 p-3">
      {chart_type === "line" ? (
        <LineChart
          data={rows as Array<Record<string, unknown>>}
          xKey={xKey}
          series={[{ dataKey: yKey, label: yKey }]}
        />
      ) : chart_type === "pie" ? (
        <PieChart
          data={rows.map((r) => ({
            name: String(r[xKey] ?? "—"),
            value: Number(r[yKey] ?? 0),
          }))}
        />
      ) : (
        <BarChart
          data={rows as Array<Record<string, unknown>>}
          xKey={xKey}
          series={[{ dataKey: yKey, label: yKey }]}
          layout={chooseBarLayout(rows.length, xKey, rows)}
        />
      )}
    </div>
  );
}

// Plain HTML table for chart_type="table" responses. Cap at 50 rows so
// a runaway query that the server permitted (the SQL guard already caps
// at LIMIT 1000) doesn't blow up the chat panel — show the rest only on
// demand. Columns come from the server in declared order, which is the
// order the LLM put them in, so we trust that ordering.
function ResultTable({
  rows,
  columns,
}: {
  rows: ReadonlyArray<Record<string, unknown>>;
  columns: ReadonlyArray<string>;
}) {
  const VISIBLE_LIMIT = 50;
  const truncated = rows.length > VISIBLE_LIMIT;
  const shown = truncated ? rows.slice(0, VISIBLE_LIMIT) : rows;
  // Defensive: if columns is empty for some reason (older server build),
  // derive them from the first row so we still render something.
  const cols = columns.length > 0 ? columns : Object.keys(shown[0] ?? {});

  return (
    <div>
      <table className="w-full min-w-[20rem] border-collapse text-xs">
        <thead>
          <tr className="text-left text-slate-500">
            {cols.map((c) => (
              <th
                key={c}
                className="border-b border-slate-200 px-2 py-1.5 font-medium uppercase tracking-wider"
              >
                {c}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {shown.map((r, i) => (
            <tr key={i} className="odd:bg-white even:bg-slate-50">
              {cols.map((c) => (
                <td key={c} className="border-b border-slate-100 px-2 py-1.5 text-slate-800">
                  {formatCell(r[c])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {truncated ? (
        <p className="mt-2 text-[11px] text-slate-400">
          Showing first {VISIBLE_LIMIT} of {rows.length} rows.
        </p>
      ) : null}
    </div>
  );
}

function formatCell(v: unknown): string {
  if (v === null || v === undefined) return "—";
  if (typeof v === "number") {
    // Compact thousands separator for readability. Integers get no decimals;
    // floats keep up to 4 sig figs so a 0.732 doesn't read as "1".
    if (Number.isInteger(v)) return v.toLocaleString();
    return v.toLocaleString(undefined, { maximumFractionDigits: 4 });
  }
  if (typeof v === "boolean") return v ? "yes" : "no";
  return String(v);
}

function chooseBarLayout(
  n: number,
  xKey: string,
  rows: ReadonlyArray<Record<string, unknown>>,
): "horizontal" | "vertical" {
  if (n > 6) return "vertical";
  const maxLen = Math.max(...rows.map((r) => String(r[xKey] ?? "").length));
  return maxLen > 10 ? "vertical" : "horizontal";
}

// ---------------------------------------------------------------------------
// Error copy
// ---------------------------------------------------------------------------

function prettyStage(stage: AIQueryError["stage"]): string {
  switch (stage) {
    case "sql_guard":
      return "I couldn't form a safe query.";
    case "tenant_filter":
      return "Tenant check failed.";
    case "llm":
      return "Upstream LLM provider is having trouble.";
    case "execution":
      return "Database error.";
    case "summarization":
      return "Couldn't summarise the result.";
    case "config":
      return "AI assistant isn't configured.";
  }
}

function stageHint(stage: AIQueryError["stage"]): string {
  switch (stage) {
    case "sql_guard":
    case "tenant_filter":
      return "Try rephrasing — name the metric and the time window explicitly.";
    case "llm":
      return "Transient — try again in a moment.";
    case "execution":
    case "summarization":
    case "config":
      return "Operator has been notified.";
  }
}
