import { NextResponse } from 'next/server';
import { agentClient } from '@/lib/agent-client';

export async function POST(req: Request) {
  try {
    const { messages, thread_id = "web-thread", user_id = "web-user" } = await req.json();
    const latestMessage = messages[messages.length - 1];

    if (!latestMessage || latestMessage.role !== 'user') {
      return NextResponse.json({ error: "Invalid message" }, { status: 400 });
    }

    const response = await agentClient.runTurn({
      message: latestMessage.content,
      provider: "web",
      user_id: user_id,
      thread_id: thread_id,
    });

    const assistantText = response.assistant_message || "(No message returned)";

    // Vercel useChat hook expects a basic text response if not streaming
    // It will parse the body text and assign it to the assistant's response.
    return new Response(assistantText, {
      status: 200,
      headers: {
        'Content-Type': 'text/plain; charset=utf-8',
      },
    });
  } catch (error: any) {
    console.error("Chat Route Error:", error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
