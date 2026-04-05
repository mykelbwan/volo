import type { AgentTurnRequest } from "../../types/agent";
import type { SessionState } from "../../types/session";

interface BuildAgentTurnRequestParams {
    messageText: string;
    provider: string;
    userId: string;
    username?: string;
    updateId: number;
    session: SessionState;
}

export function buildAgentTurnRequest(
    params: BuildAgentTurnRequestParams,
): AgentTurnRequest {
    const payload: AgentTurnRequest = {
        message: params.messageText,
        provider: params.provider,
        user_id: params.userId,
        thread_id: params.session.threadId,
        client_message_id: String(params.updateId),
    };

    if (params.username) {
        payload.username = params.username;
    }
    if (params.session.conversationId) {
        payload.conversation_id = params.session.conversationId;
    }
    if (params.session.selectedTaskNumber !== undefined) {
        payload.selected_task_number = params.session.selectedTaskNumber;
    }

    return payload;
}
