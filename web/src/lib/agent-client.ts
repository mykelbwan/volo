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
    assistant_message?: string | null;
    conversation_id: string;
    thread_id: string;
    selected_task_number?: number | null;
    allocated_new_thread: boolean;
    blocked: boolean;
    blocked_message?: string | null;
}

export class HttpAgentClient {
    private readonly turnUrl: string;

    constructor() {
        const mainEntry = process.env.MAIN_ENTRY || "http://127.0.0.1:8080";
        this.turnUrl = buildTurnUrl(mainEntry);
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
            throw new Error(
                `Agent API turn request failed for ${this.turnUrl}: ${(error as Error).message}`,
            );
        }
    }
}

export const agentClient = new HttpAgentClient();

function buildTurnUrl(mainEntry: string): string {
    const cleaned = mainEntry.replace(/\/+$/, "");
    if (cleaned.endsWith("/v1/agent/turn")) {
        return cleaned;
    }
    return `${cleaned}/v1/agent/turn`;
}
