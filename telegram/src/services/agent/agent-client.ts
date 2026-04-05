import type { AgentTurnRequest, AgentTurnResponse } from "../../types/agent";

export interface AgentClient {
    runTurn(payload: AgentTurnRequest): Promise<AgentTurnResponse>;
}
