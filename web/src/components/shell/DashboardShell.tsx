import { useCallback, useEffect, useState } from "react";
import { Outlet, useLocation, useNavigate } from "react-router-dom";
import { Sidebar } from "./Sidebar";
import { TopBar } from "./TopBar";
import { AssistantFAB } from "./AssistantFAB";
import { ChatPanel } from "@/ai/ChatPanel";
import { ShortcutsHelp } from "@/shortcuts/ShortcutsHelp";
import { useShortcuts } from "@/shortcuts/useShortcuts";
import { cn } from "@/lib/cn";

const SIDEBAR_PREF_KEY = "accent.sidebar.collapsed";

// App shell. Wraps every authenticated route. Owns:
//   - Sidebar collapse state (persisted to localStorage)
//   - Global keyboard shortcuts (assistant toggle, ? help, g-prefixed nav)
//   - Floating action button visibility (hidden on /ai where the
//     full-page assistant already lives)
//   - ChatPanel + ShortcutsHelp mount + open/closed state
//
// Shortcut wiring goes through `useShortcuts` so the help overlay's
// cheat-sheet always matches reality. The Radix Dialog inside ChatPanel
// handles Esc-to-close on its own, so we don't repeat it here.
export function DashboardShell() {
  const [collapsed, setCollapsed] = useState<boolean>(() => {
    if (typeof window === "undefined") return false;
    return localStorage.getItem(SIDEBAR_PREF_KEY) === "1";
  });
  const [assistantOpen, setAssistantOpen] = useState(false);
  const [helpOpen, setHelpOpen] = useState(false);
  const location = useLocation();
  const navigate = useNavigate();
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

  useShortcuts({
    "toggle-assistant": () => {
      // On /ai the panel is hidden (page already provides the assistant)
      // — a toggle here would just confuse.
      if (onAiPage) return;
      setAssistantOpen((v) => !v);
    },
    help: () => setHelpOpen((v) => !v),
    search: () => {
      // Focus the topbar search if present; falls back to a no-op.
      const el = document.querySelector<HTMLInputElement>(
        "[data-topbar-search]",
      );
      el?.focus();
    },
    "go-executive": () => navigate("/executive"),
    "go-operations": () => navigate("/operations"),
    "go-maintenance": () => navigate("/maintenance"),
    "go-efficiency": () => navigate("/fleet-efficiency"),
    "go-risk": () => navigate("/risk"),
    "go-safety": () => navigate("/safety"),
    "go-alerts": () => navigate("/alerts"),
    "go-ai": () => navigate("/ai"),
  });

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
      <ShortcutsHelp open={helpOpen} onOpenChange={setHelpOpen} />
    </div>
  );
}
