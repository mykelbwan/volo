export function buildDefaultThreadId(userId: string): string {
    return `tg:user:${userId}`;
}
