import type { Bot } from "grammy";

import {
    createTextMessageHandler,
    type HandleTextMessageDeps,
} from "../handlers/messages/handle-text-message";
import { sendTextReply } from "../utils/messages/send-text";

export function registerHandlers(bot: Bot, deps: HandleTextMessageDeps): void {
    bot.command("start", async (ctx) => {
        await sendTextReply(ctx, "Telegram is connected to Volo. Send a message to start.");
    });

    bot.on("message:text", createTextMessageHandler(deps));

    bot.on("message", async (ctx) => {
        await sendTextReply(ctx, "Only text messages are supported right now.");
    });
}
