import { Bot } from "grammy";

export function createBot(botToken: string): Bot {
    return new Bot(botToken);
}
