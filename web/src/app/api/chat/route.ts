import { createUIMessageStream, createUIMessageStreamResponse } from "ai";

import { agentClient } from "@/lib/agent-client";
import {
  getMessageText,
  normalizeSessionState,
  responseMessageText,
  sessionFromTurnResponse,
  type ChatSessionState,
  type VoloMessageMetadata,
  type VoloUIMessage,
} from "@/lib/volo-chat";

export const runtime = "nodejs";

interface ChatRouteBody {
  id?: string;
  messages?: VoloUIMessage[];
  session?: Partial<ChatSessionState>;
}

export async function POST(req: Request) {
  let body: ChatRouteBody;

  try {
    body = (await req.json()) as ChatRouteBody;
  } catch {
    return new Response("Invalid JSON body.", { status: 400 });
  }

  const messages = Array.isArray(body.messages) ? body.messages : [];
  const latestMessage = messages.at(-1);

  if (!latestMessage || latestMessage.role !== "user") {
    return new Response("Expected the latest message to come from the user.", {
      status: 400,
    });
  }

  const messageText = getMessageText(latestMessage).trim();
  if (!messageText) {
    return new Response("User message cannot be empty.", { status: 400 });
  }

  const session = normalizeSessionState(body.session, {
    userId: body.id,
    threadId: body.id,
  });

  try {
    const response = await agentClient.runTurn({
      message: messageText,
      provider: "web",
      user_id: session.userId,
      thread_id: session.threadId,
      conversation_id: session.conversationId,
      selected_task_number: session.selectedTaskNumber,
      client_message_id: latestMessage.id,
    });

    const nextSession = sessionFromTurnResponse(session, response);
    const assistantText = responseMessageText(response);
    const messageMetadata: VoloMessageMetadata = {
      blocked: response.blocked,
      allocatedNewThread: response.allocated_new_thread,
      threadId: nextSession.threadId,
      conversationId: nextSession.conversationId ?? response.conversation_id,
      ...(nextSession.selectedTaskNumber
        ? { selectedTaskNumber: nextSession.selectedTaskNumber }
        : {}),
    };

    const stream = createUIMessageStream<VoloUIMessage>({
      originalMessages: messages,
      execute: ({ writer }) => {
        writer.write({ type: "start" });
        writer.write({ type: "start-step" });
        writer.write({
          type: "data-session",
          data: nextSession,
        });
        writer.write({ type: "text-start", id: "text-1" });
        writer.write({
          type: "text-delta",
          id: "text-1",
          delta: assistantText,
        });
        writer.write({ type: "text-end", id: "text-1" });
        writer.write({ type: "finish-step" });
        writer.write({
          type: "finish",
          finishReason: "stop",
          messageMetadata,
        });
      },
      onError: (error) =>
        error instanceof Error ? error.message : "Chat streaming failed.",
    });

    return createUIMessageStreamResponse({ stream });
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    console.error("web_chat_turn_failed", detail);
    return new Response(detail, { status: 500 });
  }
}
