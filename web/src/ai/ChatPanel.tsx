// Slide-out chat drawer. Mounted by DashboardShell so the assistant is
// reachable from anywhere via Cmd/Ctrl+K. Backed by the same useAIChat
// hook (and therefore same localStorage) as the /ai page, so a
// conversation started in the drawer continues unchanged on the page.

import * as Dialog from "@radix-ui/react-dialog";
import { useEffect, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { Bot, ExternalLink, History, X } from "lucide-react";
import { useAuth } from "@/auth/AuthContext";
import { cn } from "@/lib/cn";
import { Button } from "@/components/ui/Button";
import { Kbd } from "@/components/ui/Kbd";
import { AssistantBody } from "./AssistantBody";
import { ConversationHistory } from "./ConversationHistory";
import { useAIChat } from "./useAIChat";

interface ChatPanelProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function ChatPanel({ open, onOpenChange }: ChatPanelProps) {
  const { user } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();
  const isSuperadmin = user?.role === "superadmin";
  const chat = useAIChat({ email: user?.email, isSuperadmin });
  const [historyOpen, setHistoryOpen] = useState(false);

  // Close the history sub-panel whenever the drawer itself closes so it
  // doesn't reappear in that state next time.
  useEffect(() => {
    if (!open) setHistoryOpen(false);
  }, [open]);

  function expandToPage() {
    onOpenChange(false);
    if (location.pathname !== "/ai") navigate("/ai");
  }

  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay
          className={cn(
            "fixed inset-0 z-40 bg-background/60 backdrop-blur-sm",
            "data-[state=open]:animate-in data-[state=closed]:animate-out",
            "data-[state=open]:fade-in-0 data-[state=closed]:fade-out-0",
          )}
        />
        <Dialog.Content
          aria-describedby={undefined}
          className={cn(
            "fixed right-0 top-0 z-50 flex h-screen w-full max-w-[28rem] flex-col",
            "border-l border-border bg-background shadow-elevated",
            "data-[state=open]:animate-in data-[state=closed]:animate-out",
            "data-[state=open]:slide-in-from-right data-[state=closed]:slide-out-to-right",
            "duration-200",
          )}
        >
          {/* Header */}
          <div className="flex shrink-0 items-center justify-between border-b border-border px-4 py-3">
            <div className="flex items-center gap-2">
              <span className="flex size-7 items-center justify-center rounded-lg bg-gradient-ai text-white shadow-sm">
                <Bot className="size-3.5" />
              </span>
              <div>
                <Dialog.Title className="text-sm font-semibold leading-none text-foreground">
                  Accent AI
                </Dialog.Title>
                <p className="mt-0.5 text-2xs text-muted-foreground">
                  Text-to-SQL assistant
                </p>
              </div>
            </div>
            <div className="flex items-center gap-1">
              <Button
                variant="ghost"
                size="icon-sm"
                onClick={() => setHistoryOpen((v) => !v)}
                aria-label="Toggle conversation history"
                title="Recent conversations"
                aria-pressed={historyOpen}
                className={cn(historyOpen && "bg-secondary")}
              >
                <History className="size-3.5" />
              </Button>
              <Button
                variant="ghost"
                size="icon-sm"
                onClick={expandToPage}
                aria-label="Open in full page"
                title="Open in full page"
              >
                <ExternalLink className="size-3.5" />
              </Button>
              <Dialog.Close asChild>
                <Button
                  variant="ghost"
                  size="icon-sm"
                  aria-label="Close assistant"
                  title="Close (Esc)"
                >
                  <X className="size-4" />
                </Button>
              </Dialog.Close>
            </div>
          </div>

          {/* Collapsible history drawer-within-drawer. Keeps the
              transcript dominant by default; one click reveals
              recents without leaving the panel. */}
          {historyOpen ? (
            <div className="max-h-[40%] shrink-0 overflow-hidden border-b border-border bg-card/40 px-3 py-3">
              <ConversationHistory
                compact
                conversations={chat.conversations.map((c) => ({
                  id: c.id,
                  title: c.title,
                  updatedAt: c.updatedAt,
                  messageCount: c.messages.length,
                }))}
                activeId={chat.activeId}
                onSelect={(id) => {
                  chat.switchTo(id);
                  setHistoryOpen(false);
                }}
                onNew={() => {
                  chat.newChat();
                  setHistoryOpen(false);
                }}
                onDelete={chat.deleteConvo}
              />
            </div>
          ) : null}

          <AssistantBody
            chat={chat}
            compact
            pathname={location.pathname}
            isSuperadmin={isSuperadmin}
            className="min-h-0 flex-1"
          />

          {/* Footer hints — keyboard shortcuts. */}
          <div className="shrink-0 border-t border-border bg-muted/30 px-4 py-2 text-2xs text-muted-foreground">
            <div className="flex items-center justify-between">
              <span className="flex items-center gap-1">
                <Kbd>Esc</Kbd> to close
              </span>
              <span className="flex items-center gap-1">
                <Kbd>⌘</Kbd>
                <Kbd>K</Kbd> to toggle
              </span>
            </div>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
