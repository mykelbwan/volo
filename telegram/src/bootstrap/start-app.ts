import { createBot } from "../bot/create-bot";
import { registerHandlers } from "../bot/register-handlers";
import { loadEnvConfig } from "../config/env";
import { HttpAgentClient } from "../services/agent/http-agent-client";
import { InMemorySessionStore } from "../state/session/in-memory-session-store";

export async function startApp(): Promise<void> {
    const config = loadEnvConfig();
    const bot = createBot(config.botToken);
    const agentClient = new HttpAgentClient(
        config.agentTurnUrl,
        config.requestTimeoutMs,
    );
    const sessionStore = new InMemorySessionStore();

    registerHandlers(bot, {
        provider: config.provider,
        agentClient,
        sessionStore,
    });

    bot.catch((error) => {
        console.error("telegram_handler_error", error.error);
    });

    await bot.start({
        onStart: (botInfo) => {
            console.log(`Telegram bot started as @${botInfo.username}`);
            console.log(`Using turn endpoint: ${config.agentTurnUrl}`);
        },
    });
}
