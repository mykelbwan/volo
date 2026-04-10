export interface AgentTurnRequest {
    message: string;
    provider: string;
    user_id: string;
    username?: string;
    thread_id: string;
    conversation_id?: string;
    selected_task_number?: number;
    client_message_id?: string;
    client_nonce?: string;
}

export interface AgentTurnResponse {
    assistant_message?: string;
    conversation_id: string;
    thread_id: string;
    selected_task_number?: number;
    allocated_new_thread: boolean;
    blocked: boolean;
    blocked_message?: string;
}

export class HttpAgentClient {
    private readonly turnUrl: string;

    constructor() {
        // Read endpoint from env, default to 8080 if not defined
        const baseUrl = process.env.MAIN_ENTRY || "http://127.0.0.1:8080";
        this.turnUrl = `${baseUrl}/v1/agent/turn`;
    }

    async runTurn(payload: AgentTurnRequest): Promise<AgentTurnResponse> {
        try {
            const response = await fetch(this.turnUrl, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                },
                body: JSON.stringify(payload),
            });

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`status=${response.status} detail=${errorText}`);
            }

            return (await response.json()) as AgentTurnResponse;
        } catch (error) {
            throw new Error(`Agent API turn request failed: ${(error as Error).message}`);
        }
    }
}

export const agentClient = new HttpAgentClient();
