export interface AgentTurnRequest {
    message: string;
    provider: string;
    user_id: string;
    thread_id: string;
    username?: string;
    conversation_id?: string;
    selected_task_number?: number;
    client_message_id?: string;
    client_nonce?: string;
}

export interface AgentTurnResponse {
    assistant_message: string | null;
    conversation_id: string;
    thread_id: string;
    selected_task_number: number | null;
    allocated_new_thread: boolean;
    blocked: boolean;
    blocked_message: string | null;
}
