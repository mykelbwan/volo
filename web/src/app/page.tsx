"use client";

import { useChat } from '@ai-sdk/react';
import { Send, Bot, User, Sparkles } from 'lucide-react';
import { useEffect, useRef, useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';

export default function Chat() {
  const [input, setInput] = useState('');
  const { messages, sendMessage, status } = useChat();
  const isLoading = status === 'submitted' || status === 'streaming';
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const onSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!input.trim() || isLoading) return;
    // @ts-ignore
    sendMessage({ text: input });
    setInput('');
  };

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  return (
    <div className="chat-container">
      <header className="header">
        <div className="w-8 h-8 rounded-full bg-white flex items-center justify-center">
          <Sparkles className="text-black w-4 h-4" />
        </div>
        <h1>Volo Agent</h1>
      </header>

      <div className="messages-container">
        <AnimatePresence>
          {messages.length === 0 && !isLoading && (
            <motion.div 
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              className="m-auto text-center mt-20 opacity-50"
              style={{ color: 'var(--text-secondary)' }}
            >
              <Sparkles className="w-12 h-12 mx-auto mb-4 opacity-30" />
              <p>How can I help you today?</p>
            </motion.div>
          )}

          {messages.map(m => {
            // @ts-ignore
            const content = m.text || m.content || (m.parts && m.parts[0]?.text) || '';
            return (
            <motion.div 
              key={m.id} 
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              className={`message-wrapper ${m.role === 'user' ? 'user' : 'assistant'}`}
            >
              <div className="message-avatar">
                {m.role === 'user' ? <User className="w-3 h-3" /> : <Bot className="w-3 h-3" />}
                <span>{m.role === 'user' ? 'You' : 'Volo'}</span>
              </div>
              <div className="message-bubble">
                {content}
              </div>
            </motion.div>
          )})}
          
          {isLoading && (
            <motion.div 
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              className="message-wrapper assistant fade-up"
            >
              <div className="message-avatar">
                <Bot className="w-3 h-3" />
                <span>Volo</span>
              </div>
              <div className="loading-dots">
                <div className="dot"></div>
                <div className="dot"></div>
                <div className="dot"></div>
              </div>
            </motion.div>
          )}
        </AnimatePresence>
        <div ref={messagesEndRef} />
      </div>

      <div className="input-container">
        <form onSubmit={onSubmit} className="input-form">
          <input
            className="chat-input"
            value={input}
            placeholder="Type your message..."
            onChange={(e) => setInput(e.target.value)}
            disabled={isLoading}
          />
          <button 
            type="submit" 
            className="send-button"
            disabled={isLoading || !input.trim()}
          >
            <Send className="w-4 h-4" />
          </button>
        </form>
      </div>
    </div>
  );
}
