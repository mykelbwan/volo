import type { Context } from "grammy";

import { buildAgentTurnRequest } from "../../mappers/telegram/agent-request";
import type { AgentClient } from "../../services/agent/agent-client";
import type { SessionStore } from "../../state/session/session-store";
import type { AgentTurnResponse } from "../../types/agent";
import { sendTextReply } from "../../utils/messages/send-text";

export interface HandleTextMessageDeps {
    provider: string;
    agentClient: AgentClient;
    sessionStore: SessionStore;
}

function responseMessage(response: AgentTurnResponse): string {
    if (response.blocked) {
        return (
            response.blocked_message ??
            response.assistant_message ??
            "Request blocked."
        );
    }

    return response.assistant_message ?? "No response generated.";
}

export function createTextMessageHandler(deps: HandleTextMessageDeps) {
    return async (ctx: Context): Promise<void> => {
        const from = ctx.from;
        const messageText = ctx.message && "text" in ctx.message ? ctx.message.text : undefined;
        if (!from || !messageText) {
            return;
        }

        const trimmed = messageText.trim();
        if (!trimmed) {
            return;
        }

        const userId = String(from.id);
        const session = deps.sessionStore.getOrCreate(userId);
        const request = buildAgentTurnRequest({
            messageText: trimmed,
            provider: deps.provider,
            userId,
            updateId: ctx.update.update_id,
            session,
            ...(from.username ? { username: from.username } : {}),
        });

        try {
            const response = await deps.agentClient.runTurn(request);
            deps.sessionStore.updateFromResponse(userId, response);
            await sendTextReply(ctx, responseMessage(response));
        } catch (error) {
            const detail = error instanceof Error ? error.message : String(error);
            console.error("telegram_turn_failed", detail);
            await sendTextReply(
                ctx,
                "I could not reach the Volo turn endpoint right now. Please retry.",
            );
        }
    };
}
