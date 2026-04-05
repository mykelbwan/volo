import type { Context } from "grammy";

const TELEGRAM_TEXT_LIMIT = 1000;

function chunkText(text: string, maxLength: number): string[] {
    const normalized = text.trim();
    if (!normalized) {
        return [];
    }
    if (normalized.length <= maxLength) {
        return [normalized];
    }

    const chunks: string[] = [];
    for (let i = 0; i < normalized.length; i += maxLength) {
        chunks.push(normalized.slice(i, i + maxLength));
    }
    return chunks;
}

export async function sendTextReply(ctx: Context, text: string): Promise<void> {
    const chunks = chunkText(text, TELEGRAM_TEXT_LIMIT);
    if (!chunks.length) {
        return;
    }

    for (const chunk of chunks) {
        await ctx.reply(chunk);
    }
}
