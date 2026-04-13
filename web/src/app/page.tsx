"use client";

import { useChat } from "@ai-sdk/react";
import { DefaultChatTransport } from "ai";
import {
  ArrowUpRight,
  Bot,
  CircleDot,
  LoaderCircle,
  RefreshCcw,
  ShieldAlert,
  Sparkles,
  User,
} from "lucide-react";
import {
  startTransition,
  useEffect,
  useRef,
  useState,
  useSyncExternalStore,
  type FormEvent,
  type KeyboardEvent,
} from "react";

import {
  createSessionState,
  getMessageText,
  normalizeSessionState,
  type ChatSessionState,
  type VoloUIMessage,
} from "@/lib/volo-chat";

const SESSION_STORAGE_KEY = "volo.web.session.v1";
const MESSAGE_STORAGE_KEY = "volo.web.messages.v1";
const STARTER_PROMPTS = [
  "Check my wallet balances across supported chains.",
  "Bridge 50 USDC from Base to Arbitrum.",
  "Swap 0.1 ETH into USDC on Base.",
];

interface BootstrapState {
  session: ChatSessionState;
  messages: VoloUIMessage[];
}

function subscribeToClientReady(): () => void {
  return () => {};
}

function readStoredSession(): ChatSessionState {
  try {
    const raw = window.localStorage.getItem(SESSION_STORAGE_KEY);
    if (!raw) {
      return createSessionState();
    }

    return normalizeSessionState(JSON.parse(raw) as Partial<ChatSessionState>);
  } catch {
    return createSessionState();
  }
}

function readStoredMessages(): VoloUIMessage[] {
  try {
    const raw = window.localStorage.getItem(MESSAGE_STORAGE_KEY);
    if (!raw) {
      return [];
    }

    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? (parsed as VoloUIMessage[]) : [];
  } catch {
    return [];
  }
}

function persistValue(key: string, value: unknown): void {
  try {
    window.localStorage.setItem(key, JSON.stringify(value));
  } catch {
    // Best-effort persistence only.
  }
}

function compactId(value: string | undefined): string {
  if (!value) {
    return "pending";
  }

  if (value.length <= 18) {
    return value;
  }

  return `${value.slice(0, 8)}...${value.slice(-6)}`;
}

function messageText(message: VoloUIMessage): string {
  return getMessageText(message).trim();
}

function statusLabel(status: string): string {
  if (status === "submitted") {
    return "Sending";
  }

  if (status === "streaming") {
    return "Thinking";
  }

  if (status === "error") {
    return "Needs attention";
  }

  return "Ready";
}

export default function Page() {
  const isClient = useSyncExternalStore(
    subscribeToClientReady,
    () => true,
    () => false,
  );
  const bootState: BootstrapState | null = isClient
    ? {
        session: readStoredSession(),
        messages: readStoredMessages(),
      }
    : null;

  return (
    <main className="volo-shell">
      <div className="volo-frame">
        {bootState ? (
          <ChatSurface
            initialMessages={bootState.messages}
            initialSession={bootState.session}
          />
        ) : (
          <LoadingSurface />
        )}
      </div>
    </main>
  );
}

function LoadingSurface() {
  return (
    <>
      <aside className="hero-panel">
        <div className="brand-row">
          <div className="brand-mark">
            <Sparkles size={28} strokeWidth={2.2} />
          </div>
          <div>
            <p className="eyebrow">Volo Console</p>
            <h1 className="hero-title">Preparing your browser session.</h1>
          </div>
        </div>
        <p className="hero-copy">
          Restoring your local thread state so the web client can behave like
          the Telegram integration.
        </p>
      </aside>

      <section className="chat-panel">
        <div className="chat-loading">
          <LoaderCircle className="spin" size={20} />
          <span>Loading conversation context…</span>
        </div>
      </section>
    </>
  );
}

function ChatSurface({
  initialMessages,
  initialSession,
}: {
  initialMessages: VoloUIMessage[];
  initialSession: ChatSessionState;
}) {
  const [composer, setComposer] = useState("");
  const [session, setSession] = useState(initialSession);
  const sessionRef = useRef(initialSession);
  const [transport] = useState(
    () => new DefaultChatTransport<VoloUIMessage>({ api: "/api/chat" }),
  );
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const { clearError, error, messages, sendMessage, setMessages, status, stop } =
    useChat<VoloUIMessage>({
      transport,
      messages: initialMessages,
      onData: (part) => {
        if (part.type !== "data-session") {
          return;
        }

        setSession((current) => {
          const next = normalizeSessionState(
            part.data as Partial<ChatSessionState>,
            {
              userId: current.userId,
              threadId: current.threadId,
            },
          );
          sessionRef.current = next;
          return next;
        });
      },
      onError: (nextError) => {
        console.error("volo_chat_client_error", nextError);
      },
    });

  const isBusy = status === "submitted" || status === "streaming";
  const visibleMessages = messages.filter((message) => message.role !== "system");
  const pendingAssistantVisible = isBusy && visibleMessages.at(-1)?.role === "user";

  useEffect(() => {
    sessionRef.current = session;
    persistValue(SESSION_STORAGE_KEY, session);
  }, [session]);

  useEffect(() => {
    persistValue(MESSAGE_STORAGE_KEY, messages);
  }, [messages]);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth", block: "end" });
  }, [messages, status]);

  useEffect(() => {
    const element = textareaRef.current;
    if (!element) {
      return;
    }

    element.style.height = "0px";
    element.style.height = `${Math.min(element.scrollHeight, 180)}px`;
  }, [composer]);

  async function submitPrompt(text: string) {
    const trimmed = text.trim();
    if (!trimmed || isBusy) {
      return;
    }

    clearError();
    setComposer("");

    try {
      await sendMessage(
        { text: trimmed },
        {
          body: {
            session: sessionRef.current,
          },
        },
      );
    } catch {
      setComposer(trimmed);
    }
  }

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    await submitPrompt(composer);
  }

  async function handleStarterPrompt(prompt: string) {
    await submitPrompt(prompt);
  }

  function handleComposerKeyDown(event: KeyboardEvent<HTMLTextAreaElement>) {
    if (event.key !== "Enter" || event.shiftKey) {
      return;
    }

    event.preventDefault();
    void submitPrompt(composer);
  }

  function handleReset() {
    stop();
    clearError();

    const nextSession = createSessionState({ userId: sessionRef.current.userId });
    sessionRef.current = nextSession;

    startTransition(() => {
      setComposer("");
      setSession(nextSession);
      setMessages([]);
    });

    try {
      window.localStorage.removeItem(MESSAGE_STORAGE_KEY);
    } catch {
      // Ignore persistence failures.
    }
  }

  return (
    <>
      <aside className="hero-panel">
        <div className="brand-row">
          <div className="brand-mark">
            <Sparkles size={28} strokeWidth={2.2} />
          </div>
          <div>
            <p className="eyebrow">Volo Console</p>
            <h1 className="hero-title">
              Telegram-style agent control, now in the browser.
            </h1>
          </div>
        </div>

        <p className="hero-copy">
          This web client keeps track of the same thread, conversation, and task
          selection fields as the Telegram plugin, while using the Vercel AI SDK
          chat protocol on the frontend.
        </p>

        <div className="status-grid">
          <div className="stat-card">
            <span className="stat-label">Thread</span>
            <strong className="stat-value">{compactId(session.threadId)}</strong>
          </div>
          <div className="stat-card">
            <span className="stat-label">Conversation</span>
            <strong className="stat-value">
              {compactId(session.conversationId)}
            </strong>
          </div>
          <div className="stat-card">
            <span className="stat-label">Selected task</span>
            <strong className="stat-value">
              {session.selectedTaskNumber
                ? `#${session.selectedTaskNumber}`
                : "None"}
            </strong>
          </div>
        </div>

        <div className="prompt-section">
          <p className="eyebrow">Jump In</p>
          <div className="prompt-list">
            {STARTER_PROMPTS.map((prompt) => (
              <button
                key={prompt}
                className="prompt-chip"
                disabled={isBusy}
                onClick={() => void handleStarterPrompt(prompt)}
                type="button"
              >
                {prompt}
              </button>
            ))}
          </div>
        </div>

        <div className="feature-list">
          <div className="feature-card">
            <p className="feature-title">Task-aware threads</p>
            <p className="feature-copy">
              Selected task numbers are persisted between turns, so follow-ups
              stay attached to the right execution lane.
            </p>
          </div>
          <div className="feature-card">
            <p className="feature-title">Vercel AI transport</p>
            <p className="feature-copy">
              The UI uses `useChat`, while the route adapts SDK messages to the
              existing `/v1/agent/turn` endpoint.
            </p>
          </div>
        </div>
      </aside>

      <section className="chat-panel">
        <header className="chat-header">
          <div>
            <p className="eyebrow">Live Session</p>
            <h2 className="chat-title">Browser console</h2>
          </div>

          <div className="chat-actions">
            <span className={`presence-chip presence-${status}`}>
              {isBusy ? (
                <LoaderCircle className="spin" size={14} />
              ) : (
                <CircleDot size={14} />
              )}
              {statusLabel(status)}
            </span>
            <button className="reset-button" onClick={handleReset} type="button">
              <RefreshCcw size={14} />
              New thread
            </button>
          </div>
        </header>

        {error ? (
          <div className="error-banner">
            <ShieldAlert size={16} />
            <span>{error.message}</span>
          </div>
        ) : null}

        <div aria-live="polite" className="chat-scroll">
          {visibleMessages.length === 0 ? (
            <div className="empty-state">
              <p className="eyebrow">Ready to help</p>
              <h3 className="empty-title">Ask Volo to inspect balances, swap, bridge, or manage a task.</h3>
              <p className="empty-copy">
                Your browser session starts with its own `thread_id`, then keeps
                the conversation and task metadata in sync with the backend after
                each turn.
              </p>
            </div>
          ) : (
            <ol className="message-list">
              {visibleMessages.map((message, index) => {
                const isUser = message.role === "user";
                const isBlocked = !isUser && Boolean(message.metadata?.blocked);
                const isStreaming =
                  !isUser &&
                  index === visibleMessages.length - 1 &&
                  status === "streaming";
                const text = messageText(message);

                return (
                  <li
                    className={[
                      "message-row",
                      isUser ? "message-user" : "message-assistant",
                      isBlocked ? "message-blocked" : "",
                      isStreaming ? "message-streaming" : "",
                    ]
                      .filter(Boolean)
                      .join(" ")}
                    key={message.id}
                  >
                    <div className="message-meta">
                      <span className="message-author">
                        {isUser ? <User size={14} /> : <Bot size={14} />}
                        {isUser ? "You" : "Volo"}
                      </span>
                      {!isUser && message.metadata?.allocatedNewThread ? (
                        <span className="message-tag">New thread</span>
                      ) : null}
                      {!isUser && message.metadata?.selectedTaskNumber ? (
                        <span className="message-tag">
                          Task #{message.metadata.selectedTaskNumber}
                        </span>
                      ) : null}
                      {isBlocked ? (
                        <span className="message-tag message-tag-warning">
                          Action required
                        </span>
                      ) : null}
                    </div>

                    <div className="message-bubble">
                      {text || (isStreaming ? "Working…" : "No text returned.")}
                    </div>
                  </li>
                );
              })}

              {pendingAssistantVisible ? (
                <li className="message-row message-assistant message-pending">
                  <div className="message-meta">
                    <span className="message-author">
                      <Bot size={14} />
                      Volo
                    </span>
                  </div>
                  <div className="message-bubble">Handing the turn to Volo…</div>
                </li>
              ) : null}
            </ol>
          )}

          <div ref={messagesEndRef} />
        </div>

        <form className="composer" onSubmit={handleSubmit}>
          <textarea
            className="composer-input"
            disabled={isBusy}
            onChange={(event) => setComposer(event.target.value)}
            onKeyDown={handleComposerKeyDown}
            placeholder="Ask Volo to inspect balances, bridge funds, or continue a task…"
            ref={textareaRef}
            rows={1}
            value={composer}
          />

          <div className="composer-footer">
            <span className="composer-hint">
              Powered by the Vercel AI SDK transport over your Volo backend.
            </span>

            <button
              className="send-button"
              disabled={isBusy || composer.trim().length === 0}
              type="submit"
            >
              <ArrowUpRight size={18} />
            </button>
          </div>
        </form>
      </section>
    </>
  );
}
