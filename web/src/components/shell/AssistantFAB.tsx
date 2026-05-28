import { useLocation } from "react-router-dom";
import { Sparkles } from "lucide-react";
import { cn } from "@/lib/cn";

interface AssistantFABProps {
  onClick: () => void;
  unreadCount?: number;
}

// Floating action button for the AI assistant. Hidden on /ai (where the
// full-page chat already lives) and on /login. Pulses softly to draw
// the eye without being annoying.
export function AssistantFAB({ onClick, unreadCount = 0 }: AssistantFABProps) {
  const { pathname } = useLocation();
  if (pathname.startsWith("/ai") || pathname.startsWith("/login")) return null;

  return (
    <button
      type="button"
      onClick={onClick}
      aria-label="Open AI assistant"
      className={cn(
        "fixed bottom-6 right-6 z-40 flex items-center gap-2 rounded-full",
        "bg-gradient-ai px-5 py-3 text-sm font-medium text-white",
        "shadow-ai-glow transition-transform duration-200 hover:scale-105 active:scale-95",
        "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 focus-visible:ring-offset-background",
      )}
    >
      <span className="relative flex">
        <Sparkles className="size-4" />
        <span className="absolute -right-1 -top-1 size-2 animate-pulse-soft rounded-full bg-white/80" />
      </span>
      <span className="hidden sm:inline">Ask AI</span>
      {unreadCount > 0 ? (
        <span className="rounded-full bg-white/25 px-1.5 text-2xs font-semibold">
          {unreadCount}
        </span>
      ) : null}
    </button>
  );
}
