import type { UIMessage } from "ai";

import type { AgentTurnResponse } from "@/lib/agent-client";

export interface ChatSessionState {
  userId: string;
  threadId: string;
  conversationId?: string;
  selectedTaskNumber?: number;
}

export interface VoloMessageMetadata {
  blocked: boolean;
  allocatedNewThread: boolean;
  threadId: string;
  conversationId: string;
  selectedTaskNumber?: number;
}

export interface VoloUIData extends Record<string, unknown> {
  session: ChatSessionState;
}

export type VoloUIMessage = UIMessage<VoloMessageMetadata, VoloUIData>;

function cleanString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }

  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function cleanSelectedTaskNumber(value: unknown): number | undefined {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 1) {
    return undefined;
  }

  return value;
}

export function createScopedId(prefix: string): string {
  return `${prefix}-${crypto.randomUUID()}`;
}

export function createSessionState(seed?: {
  userId?: string;
  threadId?: string;
}): ChatSessionState {
  return {
    userId: cleanString(seed?.userId) ?? createScopedId("web-user"),
    threadId: cleanString(seed?.threadId) ?? createScopedId("web-thread"),
  };
}

export function normalizeSessionState(
  value: Partial<ChatSessionState> | null | undefined,
  fallback?: {
    userId?: string;
    threadId?: string;
  },
): ChatSessionState {
  const base = createSessionState(fallback);
  const next: ChatSessionState = {
    userId: cleanString(value?.userId) ?? base.userId,
    threadId: cleanString(value?.threadId) ?? base.threadId,
  };

  const conversationId = cleanString(value?.conversationId);
  if (conversationId) {
    next.conversationId = conversationId;
  }

  const selectedTaskNumber = cleanSelectedTaskNumber(value?.selectedTaskNumber);
  if (selectedTaskNumber) {
    next.selectedTaskNumber = selectedTaskNumber;
  }

  return next;
}

export function sessionFromTurnResponse(
  current: ChatSessionState,
  response: AgentTurnResponse,
): ChatSessionState {
  const next: ChatSessionState = {
    userId: current.userId,
    threadId: cleanString(response.thread_id) ?? current.threadId,
    conversationId:
      cleanString(response.conversation_id) ?? current.conversationId,
  };

  const selectedTaskNumber = cleanSelectedTaskNumber(
    response.selected_task_number,
  );
  if (selectedTaskNumber) {
    next.selectedTaskNumber = selectedTaskNumber;
  }

  return next;
}

export function responseMessageText(response: AgentTurnResponse): string {
  if (response.blocked) {
    return (
      response.blocked_message ??
      response.assistant_message ??
      "Request blocked."
    );
  }

  return response.assistant_message ?? "No response generated.";
}

export function getMessageText(
  message: Pick<UIMessage, "parts"> | undefined,
): string {
  if (!message) {
    return "";
  }

  return message.parts
    .filter((part) => part.type === "text")
    .map((part) => part.text)
    .join("");
}
