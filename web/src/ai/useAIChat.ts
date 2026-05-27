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
  type AIQueryResponse,
  type ChatTurn,
} from "@/api/ai";

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
}

export function useAIChat({ email, isSuperadmin }: UseAIChatOptions): UseAIChatReturn {
  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [activeId, setActiveId] = useState<string | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [hydrated, setHydrated] = useState(false);

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
    setHydrated(true);
  }, [email]);

  // -------------------------------------------------------------------------
  // Persist on every meaningful change post-hydration. Empty list is a
  // no-op so the initial render after sign-in doesn't wipe stored data
  // before hydration completes; explicit deletion calls clearConvos.
  // -------------------------------------------------------------------------
  useEffect(() => {
    if (!hydrated) return;
    if (conversations.length === 0) return;
    saveConvos(email, conversations, activeId);
  }, [hydrated, email, conversations, activeId]);

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

  return {
    conversations,
    active,
    activeId,
    messages,
    tenant,
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
