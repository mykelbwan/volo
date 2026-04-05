import axios, { AxiosError, AxiosInstance } from "axios";

import type { AgentTurnRequest, AgentTurnResponse } from "../../types/agent";
import type { AgentClient } from "./agent-client";

function describeAxiosError(error: AxiosError): string {
    const status = error.response?.status;
    const responseData = error.response?.data;
    const detail =
        typeof responseData === "string"
            ? responseData
            : responseData && typeof responseData === "object"
              ? JSON.stringify(responseData)
              : error.message;
    return status ? `status=${status} detail=${detail}` : detail;
}

export class HttpAgentClient implements AgentClient {
    private readonly http: AxiosInstance;

    constructor(
        private readonly turnUrl: string,
        timeoutMs: number,
    ) {
        this.http = axios.create({ timeout: timeoutMs });
    }

    async runTurn(payload: AgentTurnRequest): Promise<AgentTurnResponse> {
        try {
            const response = await this.http.post<AgentTurnResponse>(this.turnUrl, payload);
            return response.data;
        } catch (error) {
            if (axios.isAxiosError(error)) {
                throw new Error(`Agent API turn request failed: ${describeAxiosError(error)}`);
            }
            throw error;
        }
    }
}
