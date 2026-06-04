import { useCallback, useEffect, useRef, useState } from "react";
import { Outlet, useLocation, useNavigate } from "react-router-dom";
import { Sidebar } from "./Sidebar";
import { TopBar } from "./TopBar";
import { AssistantFAB } from "./AssistantFAB";
import { ChatPanel } from "@/ai/ChatPanel";
import { ShortcutsHelp } from "@/shortcuts/ShortcutsHelp";
import { useShortcuts } from "@/shortcuts/useShortcuts";
import { cn } from "@/lib/cn";

const SIDEBAR_PREF_KEY = "accent.sidebar.collapsed";

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
  const mainRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    try {
      localStorage.setItem(SIDEBAR_PREF_KEY, collapsed ? "1" : "0");
    } catch {
      /* ignore */
    }
  }, [collapsed]);

  // Scroll to top on route change
  useEffect(() => {
    if (mainRef.current) {
      mainRef.current.scrollTop = 0;
    }
  }, [location.pathname]);

  useEffect(() => {
    if (onAiPage && assistantOpen) setAssistantOpen(false);
  }, [onAiPage, assistantOpen]);

  const openAssistant = useCallback(() => {
    setAssistantOpen(true);
  }, []);

  useShortcuts({
    "toggle-assistant": () => {
      if (onAiPage) return;
      setAssistantOpen((v) => !v);
    },
    help: () => setHelpOpen((v) => !v),
    search: () => {
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
        <main ref={mainRef} className="flex-1 overflow-y-auto">
          <Outlet />
        </main>
      </div>
      {!onAiPage ? <AssistantFAB onClick={openAssistant} /> : null}
      <ChatPanel open={assistantOpen} onOpenChange={setAssistantOpen} />
      <ShortcutsHelp open={helpOpen} onOpenChange={setHelpOpen} />
    </div>
  );
}
