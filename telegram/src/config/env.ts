import "dotenv/config";

export interface EnvConfig {
    botToken: string;
    provider: string;
    agentTurnUrl: string;
    requestTimeoutMs: number;
}

function requireEnv(name: string): string {
    const value = process.env[name];
    if (!value || !value.trim()) {
        throw new Error(`${name} environment variable is not set`);
    }
    return value.trim();
}

function parseTimeoutMs(raw: string | undefined): number {
    if (!raw || !raw.trim()) {
        return 60000;
    }
    const value = Number.parseInt(raw, 10);
    if (!Number.isFinite(value) || value <= 0) {
        throw new Error("AGENT_REQUEST_TIMEOUT_MS must be a positive integer");
    }
    return value;
}

function buildTurnUrl(mainEntry: string): string {
    const cleaned = mainEntry.replace(/\/+$/, "");
    if (cleaned.endsWith("/v1/agent/turn")) {
        return cleaned;
    }
    return `${cleaned}/v1/agent/turn`;
}

export function loadEnvConfig(): EnvConfig {
    const botToken = requireEnv("BOT_TOKEN");
    const mainEntry = requireEnv("MAIN_ENTRY");

    return {
        botToken,
        provider: "telegram",
        agentTurnUrl: buildTurnUrl(mainEntry),
        requestTimeoutMs: parseTimeoutMs(process.env.AGENT_REQUEST_TIMEOUT_MS),
    };
}
