// AI chat page — Phase 2.
//
// The conversation state machine, localStorage persistence (v2 schema:
// `accent.ai.chats.v2:<email>`), tenant guard, and race-safe convoId
// pattern that used to live inline have moved into useAIChat. The
// rendering composition lives in FullPageAssistant + AssistantBody,
// shared with the slide-out ChatPanel so both surfaces stay in sync.
//
// Keeping this re-export (rather than pointing the route directly at
// FullPageAssistant) preserves the `pages/AIChat.tsx → AIChat`
// component name in router-config and devtools.

import { FullPageAssistant } from "@/ai/FullPageAssistant";

const BUILD_TAG = "AIChat v5.0 — phase-2 redesign";
if (typeof window !== "undefined") {
  // eslint-disable-next-line no-console
  console.info(`[${BUILD_TAG}] loaded`);
}

export function AIChat() {
  return <FullPageAssistant />;
}
