import type { AgentTurnResponse } from "../../types/agent";
import type { SessionState } from "../../types/session";

export interface SessionStore {
    getOrCreate(userId: string): SessionState;
    updateFromResponse(userId: string, response: AgentTurnResponse): SessionState;
}
