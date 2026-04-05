import type { AgentTurnResponse } from "../../types/agent";
import type { SessionState } from "../../types/session";
import { buildDefaultThreadId } from "../../utils/thread/default-thread-id";
import type { SessionStore } from "./session-store";

export class InMemorySessionStore implements SessionStore {
    private readonly sessions = new Map<string, SessionState>();

    getOrCreate(userId: string): SessionState {
        const key = String(userId);
        const existing = this.sessions.get(key);
        if (existing) {
            return existing;
        }

        const created: SessionState = {
            threadId: buildDefaultThreadId(key),
        };
        this.sessions.set(key, created);
        return created;
    }

    updateFromResponse(userId: string, response: AgentTurnResponse): SessionState {
        const current = this.getOrCreate(userId);
        const next: SessionState = {
            ...current,
            conversationId: response.conversation_id,
        };

        if (response.selected_task_number !== null) {
            next.selectedTaskNumber = response.selected_task_number;
        } else {
            delete next.selectedTaskNumber;
        }

        this.sessions.set(String(userId), next);
        return next;
    }
}
