import { useCallback, useEffect, useState } from "react";
import { Outlet, useLocation } from "react-router-dom";
import { Sidebar } from "./Sidebar";
import { TopBar } from "./TopBar";
import { AssistantFAB } from "./AssistantFAB";
import { ChatPanel } from "@/ai/ChatPanel";
import { cn } from "@/lib/cn";

const SIDEBAR_PREF_KEY = "accent.sidebar.collapsed";

// App shell. Wraps every authenticated route. Owns:
//   - Sidebar collapse state (persisted to localStorage)
//   - Cmd/Ctrl-K shortcut to toggle the ChatPanel slide-out
//   - Floating action button visibility (hidden on /ai where the
//     full-page assistant already lives)
//   - ChatPanel mount + open/closed state
//
// The Radix Dialog inside ChatPanel handles Esc-to-close on its own,
// so we don't repeat it here.
export function DashboardShell() {
  const [collapsed, setCollapsed] = useState<boolean>(() => {
    if (typeof window === "undefined") return false;
    return localStorage.getItem(SIDEBAR_PREF_KEY) === "1";
  });
  const [assistantOpen, setAssistantOpen] = useState(false);
  const location = useLocation();
  const onAiPage = location.pathname.startsWith("/ai");

  useEffect(() => {
    try {
      localStorage.setItem(SIDEBAR_PREF_KEY, collapsed ? "1" : "0");
    } catch {
      /* ignore */
    }
  }, [collapsed]);

  // Auto-close the slide-out when the user is on /ai — the full-page
  // assistant is already showing the same conversation, so the drawer
  // would just be visual noise on top of itself.
  useEffect(() => {
    if (onAiPage && assistantOpen) setAssistantOpen(false);
  }, [onAiPage, assistantOpen]);

  const openAssistant = useCallback(() => {
    setAssistantOpen(true);
  }, []);

  // Cmd/Ctrl+K toggles the panel. We don't fight Radix's Esc-to-close;
  // it already handles that via the Dialog primitive. We DO skip the
  // shortcut when the focus is in an editable element that uses it
  // (CodeMirror's default save binding is ⌘S, not ⌘K, so this is
  // safe — but if we later add a ⌘K binding inside CodeMirror this
  // needs revisiting).
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      const mod = e.metaKey || e.ctrlKey;
      if (mod && e.key.toLowerCase() === "k") {
        e.preventDefault();
        // On /ai, ⌘K is a no-op (panel is hidden because the page
        // already provides the assistant) — would just confuse.
        if (onAiPage) return;
        setAssistantOpen((v) => !v);
      }
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onAiPage]);

  return (
    <div className="flex h-screen w-full overflow-hidden bg-background text-foreground">
      <Sidebar collapsed={collapsed} onToggle={() => setCollapsed((c) => !c)} />
      <div className={cn("flex min-w-0 flex-1 flex-col")}>
        <TopBar onOpenAssistant={openAssistant} />
        <main className="flex-1 overflow-y-auto">
          <Outlet />
        </main>
      </div>
      {!onAiPage ? <AssistantFAB onClick={openAssistant} /> : null}
      <ChatPanel open={assistantOpen} onOpenChange={setAssistantOpen} />
    </div>
  );
}
