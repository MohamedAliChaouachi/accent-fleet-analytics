// useAIChat — state machine + persistence for the AI assistant.
//
// Encapsulates the model that lived inside AIChat.tsx v4.x so both
// the slide-out ChatPanel and the full-page FullPageAssistant can
// share it. Specifically preserves three behaviors from the original:
//
//   1. v2 per-user localStorage schema: `accent.ai.chats.v2:<email>` →
//      `{ v: 2, conversations: [...], activeId }`. Bumping `v` would
//      silently drop older shapes (we don't migrate forward — there's
//      nothing valuable in v1 single-chat layout).
//
//   2. Race-safe convoId capture in `send()`. We resolve the target
//      conversation BEFORE calling setState/mutate so onSuccess writes
//      back to the right convo even if the user switches chats while
//      a request is in flight. This was the v3.x bug — onSuccess
//      matched against a stale `activeId` and silently dropped
//      assistant replies.
//
//   3. Tenant guard is SOFT for superadmins. Send isn't disabled on
//      missing tenant; we surface an inline error instead so the
//      button never visually disappears.
//
// Transient errors are NOT persisted — they live in component state
// and are dropped on conversation switch / next successful send.

import { useMutation } from "@tanstack/react-query";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  askAI,
  AIQueryError,
  MAX_HISTORY_TURNS,
  fetchAIHistory,
  postAIFeedback,
  type AIFeedbackValue,
  type AIQueryResponse,
  type ChatTurn,
} from "@/api/ai";

import type { FeedbackValue } from "./QueryFeedback";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type DisplayMessage =
  | { id: number; role: "user"; content: string; createdAt: number }
  | {
      id: number;
      role: "assistant";
      response: AIQueryResponse;
      createdAt: number;
    };

export interface Conversation {
  id: string;
  title: string;
  messages: DisplayMessage[];
  tenant: string;
  updatedAt: number;
  nextMsgId: number;
}

interface PersistedStateV2 {
  v: 2;
  conversations: Conversation[];
  activeId: string | null;
  // Feedback by server event_id, persisted so a refresh shows the right
  // thumb state even before the server-side /history sync lands.
  // Optional for forward-compat: older bundles that never saved it will
  // read it back as undefined and fall through to the server fetch.
  feedback?: Record<number, FeedbackValue>;
}

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
  feedback: Record<number, FeedbackValue>,
): void {
  const key = storageKeyFor(email);
  if (!key) return;
  try {
    const body: PersistedStateV2 = {
      v: STORAGE_VERSION,
      conversations: conversations as Conversation[],
      activeId,
      feedback,
    };
    window.localStorage.setItem(key, JSON.stringify(body));
  } catch {
    /* quota — drop silently */
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

// ---------------------------------------------------------------------------
// Helpers
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

// ---------------------------------------------------------------------------
// Hook
// ---------------------------------------------------------------------------

export interface UseAIChatOptions {
  /** User email, used to scope localStorage. Pass undefined while loading. */
  email: string | undefined;
  /** Whether the current user is a superadmin (controls tenant requirement). */
  isSuperadmin: boolean;
}

export interface UseAIChatReturn {
  // Data
  conversations: ReadonlyArray<Conversation>;
  active: Conversation | null;
  activeId: string | null;
  messages: ReadonlyArray<DisplayMessage>;
  tenant: string;
  /**
   * Feedback by *event_id* — server-aligned so a re-hydration on a
   * different device picks up the same thumbs state via /history sync.
   * Components that key off message id should look up the event_id from
   * the message's response first.
   */
  feedbackByEventId: Record<number, FeedbackValue>;
  // Status
  isLoading: boolean;
  error: Error | null;
  hydrated: boolean;
  // Actions
  send: (question: string) => void;
  cancel: () => void;
  newChat: () => void;
  switchTo: (id: string) => void;
  deleteConvo: (id: string) => void;
  setTenant: (tenant: string) => void;
  clearError: () => void;
  /**
   * Set feedback for one assistant message. Optimistic: updates local
   * state immediately, then POSTs in the background. Failures are
   * silent — the worst case is a stale local thumb that resyncs on the
   * next /history fetch.
   */
  setFeedback: (messageId: number, value: FeedbackValue, comment?: string) => void;
}

export function useAIChat({ email, isSuperadmin }: UseAIChatOptions): UseAIChatReturn {
  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [activeId, setActiveId] = useState<string | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [hydrated, setHydrated] = useState(false);
  const [feedbackByEventId, setFeedbackByEventId] = useState<
    Record<number, FeedbackValue>
  >({});

  // AbortController for in-flight requests so the UI can offer a Stop
  // button. Reset on each new send.
  const abortRef = useRef<AbortController | null>(null);

  // Derived
  const active = useMemo(
    () => conversations.find((c) => c.id === activeId) ?? null,
    [conversations, activeId],
  );
  const messages: ReadonlyArray<DisplayMessage> = active?.messages ?? [];
  const tenant = active?.tenant ?? "";

  // -------------------------------------------------------------------------
  // Hydrate from localStorage when the user identity becomes known.
  // -------------------------------------------------------------------------
  useEffect(() => {
    if (!email) {
      setConversations([]);
      setActiveId(null);
      setFeedbackByEventId({});
      setHydrated(true);
      return;
    }
    const saved = loadConvos(email);
    if (saved && saved.conversations.length > 0) {
      const sorted = [...saved.conversations].sort(
        (a, b) => b.updatedAt - a.updatedAt,
      );
      // Backfill createdAt on old persisted messages — schema additions
      // shouldn't crash older bundles.
      for (const c of sorted) {
        for (const m of c.messages) {
          if (typeof (m as { createdAt?: number }).createdAt !== "number") {
            (m as DisplayMessage).createdAt = c.updatedAt;
          }
        }
      }
      setConversations(sorted);
      const stillExists =
        saved.activeId && sorted.some((c) => c.id === saved.activeId);
      setActiveId(stillExists ? saved.activeId : sorted[0]!.id);
    } else {
      setConversations([]);
      setActiveId(null);
    }
    setFeedbackByEventId(saved?.feedback ?? {});
    setHydrated(true);
  }, [email]);

  // Fan-in server-side feedback once on hydrate. Server is the source of
  // truth across devices; local writes are merged on top. We never let a
  // /history failure block hydration — the cached feedback is still good.
  useEffect(() => {
    if (!hydrated || !email) return;
    let cancelled = false;
    fetchAIHistory(100)
      .then((res) => {
        if (cancelled) return;
        const merged: Record<number, FeedbackValue> = {};
        for (const item of res.items) {
          if (item.feedback_value === 1) merged[item.event_id] = "up";
          else if (item.feedback_value === -1) merged[item.event_id] = "down";
        }
        // Local edits since this fetch was kicked off win. Reading the
        // latest state inside the updater avoids a stale-closure overwrite.
        setFeedbackByEventId((local) => ({ ...merged, ...local }));
      })
      .catch(() => {
        /* offline / 401 — local state is fine */
      });
    return () => {
      cancelled = true;
    };
  }, [hydrated, email]);

  // -------------------------------------------------------------------------
  // Persist on every meaningful change post-hydration. Empty list is a
  // no-op so the initial render after sign-in doesn't wipe stored data
  // before hydration completes; explicit deletion calls clearConvos.
  // -------------------------------------------------------------------------
  useEffect(() => {
    if (!hydrated) return;
    if (conversations.length === 0) return;
    saveConvos(email, conversations, activeId, feedbackByEventId);
  }, [hydrated, email, conversations, activeId, feedbackByEventId]);

  // -------------------------------------------------------------------------
  // Mutation. Note: we pass `convoId` and `historySnapshot` through vars
  // so onSuccess writes to the conversation that was active at send time,
  // not whichever convo the user has selected when the response lands.
  // -------------------------------------------------------------------------
  const mutation = useMutation<
    AIQueryResponse,
    Error,
    { convoId: string; question: string; history: ChatTurn[]; tenantId?: number }
  >({
    mutationFn: async ({ question, history, tenantId }) => {
      const ctrl = new AbortController();
      abortRef.current = ctrl;
      try {
        return await askAI({ question, tenant_id: tenantId, history }, ctrl.signal);
      } finally {
        if (abortRef.current === ctrl) abortRef.current = null;
      }
    },
    onSuccess: (resp, vars) => {
      setConversations((prev) =>
        prev.map((c) =>
          c.id !== vars.convoId
            ? c
            : {
                ...c,
                messages: [
                  ...c.messages,
                  {
                    id: c.nextMsgId,
                    role: "assistant",
                    response: resp,
                    createdAt: Date.now(),
                  },
                ],
                nextMsgId: c.nextMsgId + 1,
                updatedAt: Date.now(),
              },
        ),
      );
      setError(null);
    },
    onError: (err) => {
      // Aborts surface as DOMException("AbortError") — silence them.
      if (err && (err.name === "AbortError" || /aborted/i.test(err.message))) {
        return;
      }
      setError(err instanceof Error ? err : new Error(String(err)));
    },
  });

  // -------------------------------------------------------------------------
  // Actions
  // -------------------------------------------------------------------------

  const send = useCallback(
    (questionRaw: string) => {
      const q = questionRaw.trim();
      if (!q || mutation.isPending) return;
      if (isSuperadmin && !tenant.trim()) {
        setError(new Error("Pick a tenant from the dropdown before sending."));
        return;
      }

      // Resolve target convo synchronously so mutation.mutate sees the
      // final id, not whatever a deferred state updater would produce.
      const existing = activeId
        ? conversations.find((c) => c.id === activeId) ?? null
        : null;
      const conversation =
        existing ?? newConversation(isSuperadmin ? tenant : "");
      const convoId = conversation.id;
      const isNew = existing === null;

      // Build history from the CURRENT (pre-append) messages.
      const history: ChatTurn[] = (existing?.messages ?? [])
        .flatMap<ChatTurn>((m) => {
          if (m.role === "user") return [{ role: "user", content: m.content }];
          return [{ role: "assistant", content: m.response.summary }];
        })
        .slice(-MAX_HISTORY_TURNS);

      setConversations((prev) => {
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
            messages: [
              ...c.messages,
              { id: c.nextMsgId, role: "user", content: q, createdAt: Date.now() },
            ],
            nextMsgId: c.nextMsgId + 1,
            updatedAt: Date.now(),
          };
        });
      });
      if (activeId !== convoId) setActiveId(convoId);
      setError(null);

      const tenantId =
        isSuperadmin && tenant.trim() ? Number(tenant.trim()) : undefined;
      mutation.mutate({ convoId, question: q, history, tenantId });
    },
    [activeId, conversations, isSuperadmin, mutation, tenant],
  );

  const cancel = useCallback(() => {
    abortRef.current?.abort();
    abortRef.current = null;
  }, []);

  const newChat = useCallback(() => {
    // Deselect instead of allocating; the first message will create
    // the convo. Avoids littering history with empty entries.
    setActiveId(null);
    setError(null);
    mutation.reset();
  }, [mutation]);

  const switchTo = useCallback(
    (id: string) => {
      if (id === activeId) return;
      setActiveId(id);
      setError(null);
      mutation.reset();
    },
    [activeId, mutation],
  );

  const deleteConvo = useCallback(
    (id: string) => {
      setConversations((prev) => {
        const remaining = prev.filter((c) => c.id !== id);
        if (id === activeId) {
          if (remaining.length === 0) {
            setActiveId(null);
            clearConvos(email);
          } else {
            const sorted = [...remaining].sort(
              (a, b) => b.updatedAt - a.updatedAt,
            );
            setActiveId(sorted[0]!.id);
          }
        }
        return remaining;
      });
    },
    [activeId, email],
  );

  const setTenant = useCallback(
    (value: string) => {
      if (!active) {
        const fresh = newConversation(value);
        setConversations((prev) => [fresh, ...prev]);
        setActiveId(fresh.id);
        return;
      }
      setConversations((prev) =>
        prev.map((c) => (c.id === active.id ? { ...c, tenant: value } : c)),
      );
    },
    [active],
  );

  const clearError = useCallback(() => setError(null), []);

  // Find the event_id for a message id within the active conversation.
  // Returns null when the message is not an assistant turn or has no
  // event_id (audit write failed) — callers should NOT POST in that case.
  const lookupEventId = useCallback(
    (messageId: number): number | null => {
      if (!active) return null;
      const msg = active.messages.find((m) => m.id === messageId);
      if (!msg || msg.role !== "assistant") return null;
      const id = msg.response.event_id;
      return typeof id === "number" ? id : null;
    },
    [active],
  );

  const setFeedback = useCallback(
    (messageId: number, value: FeedbackValue, comment?: string) => {
      const eventId = lookupEventId(messageId);
      if (eventId === null) {
        // No backend handle for this turn (e.g. audit write failed).
        // We keep no local state for orphaned feedback — UI just won't
        // render the buttons in this case (MessageList gates on the
        // presence of event_id, see below).
        return;
      }
      // Optimistic local update — UI feels instant.
      setFeedbackByEventId((prev) => {
        const next = { ...prev };
        if (value === null) delete next[eventId];
        else next[eventId] = value;
        return next;
      });
      // Server upsert. -1/1 maps directly to the API; clearing the
      // thumb (value === null) is a no-op against the backend for now
      // because the schema doesn't model "retract" — we simply leave
      // the last vote in place. Cheap to extend later with a DELETE.
      if (value === null) return;
      const apiValue: AIFeedbackValue = value === "up" ? 1 : -1;
      void postAIFeedback({
        event_id: eventId,
        value: apiValue,
        comment: comment ?? null,
      }).catch(() => {
        /* server rejected (404, 5xx) — local state stays; resync on next /history */
      });
    },
    [lookupEventId],
  );

  return {
    conversations,
    active,
    activeId,
    messages,
    tenant,
    feedbackByEventId,
    isLoading: mutation.isPending,
    error,
    hydrated,
    send,
    cancel,
    newChat,
    switchTo,
    deleteConvo,
    setTenant,
    clearError,
    setFeedback,
  };
}

// ---------------------------------------------------------------------------
// Error copy — exported so panels can share consistent messaging.
// ---------------------------------------------------------------------------

export function prettyAIError(err: Error): { title: string; hint: string } {
  if (err instanceof AIQueryError) {
    switch (err.stage) {
      case "sql_guard":
        return {
          title: "I couldn't form a safe query.",
          hint: "Try rephrasing — name the metric and time window explicitly.",
        };
      case "tenant_filter":
        return {
          title: "Tenant check failed.",
          hint: "Try rephrasing — name the metric and time window explicitly.",
        };
      case "llm":
        return {
          title: "Upstream LLM provider is having trouble.",
          hint: "Transient — try again in a moment.",
        };
      case "execution":
        return { title: "Database error.", hint: "Operator has been notified." };
      case "summarization":
        return {
          title: "Couldn't summarise the result.",
          hint: "Operator has been notified.",
        };
      case "config":
        return {
          title: "AI assistant isn't configured.",
          hint: "Operator has been notified.",
        };
    }
  }
  return { title: err.message || "Something went wrong.", hint: "Try again." };
}
