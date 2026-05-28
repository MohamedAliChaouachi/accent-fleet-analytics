// Full-page AI assistant — the body of the /ai route.
//
// Same composition as ChatPanel (uses AssistantBody under the hood)
// but without compact spacing and with the conversation history pinned
// as a right rail rather than collapsed behind a button. The two
// surfaces share state through useAIChat → localStorage so a
// conversation started in the FAB drawer continues here unchanged.

import { useLocation } from "react-router-dom";
import { Sparkles } from "lucide-react";
import { PageContainer } from "@/components/shell/PageContainer";
import { useAuth } from "@/auth/AuthContext";
import { Badge } from "@/components/ui/Badge";
import { AssistantBody } from "./AssistantBody";
import { useAIChat } from "./useAIChat";

export function FullPageAssistant() {
  const { user } = useAuth();
  const location = useLocation();
  const isSuperadmin = user?.role === "superadmin";
  const chat = useAIChat({ email: user?.email, isSuperadmin });

  return (
    <PageContainer
      title="AI Assistant"
      description="Ask natural-language questions. I generate, validate, and run the SQL."
      actions={
        <Badge variant="ai" className="gap-1">
          <Sparkles className="size-3" />
          Text-to-SQL
        </Badge>
      }
    >
      <div className="flex h-[calc(100vh-13rem)] w-full">
        <div className="flex min-w-0 flex-1 flex-col overflow-hidden rounded-2xl border border-border bg-card/40 shadow-card">
          <AssistantBody
            chat={chat}
            pathname={location.pathname}
            isSuperadmin={isSuperadmin}
          />
        </div>
      </div>
    </PageContainer>
  );
}
