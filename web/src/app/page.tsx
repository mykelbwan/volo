"use client";

import { useChat } from "@ai-sdk/react";
import { DefaultChatTransport } from "ai";
import {
  ArrowUpRight,
  Bot,
  LoaderCircle,
  RefreshCcw,
  ShieldAlert,
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
const RECOMMENDATIONS = [
  "Check my wallet balances.",
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
    return "Error";
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
    <main className="chat-app">
      {bootState ? (
        <ChatSurface
          initialMessages={bootState.messages}
          initialSession={bootState.session}
        />
      ) : (
        <LoadingSurface />
      )}
    </main>
  );
}

function LoadingSurface() {
  return (
    <section className="loading-page">
      <div className="loading-state">
        <LoaderCircle className="spin" size={18} />
        <span>Loading chat…</span>
      </div>
    </section>
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
  const showPendingAssistant = isBusy && visibleMessages.at(-1)?.role === "user";

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
    element.style.height = `${Math.min(element.scrollHeight, 160)}px`;
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
      <header className="chat-header">
        <div className="chat-header-inner">
          <div className="chat-heading-wrap">
            <p className="chat-kicker">Volo</p>
            <h1 className="chat-heading">Chat</h1>
          </div>

          <div className="chat-topbar-actions">
            <span className={`status-badge status-${status}`}>
              {isBusy ? <LoaderCircle className="spin" size={14} /> : null}
              {statusLabel(status)}
            </span>
            <button className="ghost-button" onClick={handleReset} type="button">
              <RefreshCcw size={14} />
              New chat
            </button>
          </div>
        </div>
      </header>

      {error ? (
        <div className="banner-wrap">
          <div className="error-banner">
            <ShieldAlert size={16} />
            <span>{error.message}</span>
          </div>
        </div>
      ) : null}

      <div className="chat-scroll" aria-live="polite">
        <div className="chat-content">
          {visibleMessages.length === 0 ? (
            <div className="empty-state">
              <h2 className="empty-title">Ask Volo anything.</h2>
              <p className="empty-copy">
                Wallet setup, balances, swaps, bridges, or task follow-ups.
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
                    <div className="message-label">
                      {isUser ? <User size={14} /> : <Bot size={14} />}
                      <span>{isUser ? "You" : "Volo"}</span>
                    </div>
                    <div className="message-bubble">
                      {text || (isStreaming ? "Working…" : "No text returned.")}
                    </div>
                  </li>
                );
              })}

              {showPendingAssistant ? (
                <li className="message-row message-assistant">
                  <div className="message-label">
                    <Bot size={14} />
                    <span>Volo</span>
                  </div>
                  <div className="message-bubble message-bubble-pending">
                    Thinking…
                  </div>
                </li>
              ) : null}
            </ol>
          )}

          <div ref={messagesEndRef} />
        </div>
      </div>

      <div className="composer-dock">
        <div className="composer-inner">
          <div className="recommendation-row">
            {RECOMMENDATIONS.map((recommendation) => (
              <button
                className="recommendation-chip"
                disabled={isBusy}
                key={recommendation}
                onClick={() => void submitPrompt(recommendation)}
                type="button"
              >
                {recommendation}
              </button>
            ))}
          </div>

          <form className="composer" onSubmit={handleSubmit}>
            <textarea
              className="composer-input"
              disabled={isBusy}
              onChange={(event) => setComposer(event.target.value)}
              onKeyDown={handleComposerKeyDown}
              placeholder="Message Volo…"
              ref={textareaRef}
              rows={1}
              value={composer}
            />
            <button
              className="send-button"
              disabled={isBusy || composer.trim().length === 0}
              type="submit"
            >
              <ArrowUpRight size={18} />
            </button>
          </form>
        </div>
      </div>
    </>
  );
}
