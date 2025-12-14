import { ref, onUnmounted } from "vue";
import type { Ref } from "vue";

export interface WebSocketMessage {
  type?: string;
  event_type?: string;
  document_type?: string;
  document_data?: any;
  timestamp?: string;
  message?: string;
  error?: string;
}

export interface UseWebSocketOptions {
  onMessage?: (message: WebSocketMessage) => void;
  onError?: (error: Event) => void;
  onConnect?: () => void;
  onDisconnect?: () => void;
  autoReconnect?: boolean;
  reconnectInterval?: number;
  maxReconnectAttempts?: number;
}

export function useWebSocket(householdId: string, options: UseWebSocketOptions = {}) {
  const {
    onMessage,
    onError,
    onConnect,
    onDisconnect,
    autoReconnect = true,
    reconnectInterval = 3000,
    maxReconnectAttempts = 10,
  } = options;

  const ws: Ref<WebSocket | null> = ref(null);
  const isConnected = ref(false);
  const isConnecting = ref(false);
  const reconnectAttempts = ref(0);
  const reconnectTimeout: Ref<NodeJS.Timeout | null> = ref(null);

  function getWebSocketUrl() {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const host = window.location.host;
    return `${protocol}//${host}/api/ws/households/${householdId}`;
  }

  async function connect() {
    if (isConnecting.value || isConnected.value) {
      return;
    }

    isConnecting.value = true;

    try {
      const token = useCookie("mealie.auth.token").value;
      if (!token) {
        console.error("No authentication token available");
        isConnecting.value = false;
        return;
      }

      const url = getWebSocketUrl();
      ws.value = new WebSocket(url);

      ws.value.onopen = () => {
        isConnecting.value = false;
        isConnected.value = true;
        reconnectAttempts.value = 0;

        // Send authentication token
        if (ws.value && token) {
          ws.value.send(token);
        }

        onConnect?.();
      };

      ws.value.onmessage = (event) => {
        try {
          const message: WebSocketMessage = JSON.parse(event.data);
          
          // Handle connection confirmation
          if (message.type === "connected") {
            console.log("WebSocket authenticated and connected", message);
            return;
          }

          // Handle pong response
          if (message.type === "pong") {
            return;
          }

          // Handle errors
          if (message.error) {
            console.error("WebSocket error:", message.error);
            onError?.(new Event(message.error));
            return;
          }

          // Call user-provided message handler
          onMessage?.(message);
        } catch (err) {
          console.error("Failed to parse WebSocket message:", err);
        }
      };

      ws.value.onerror = (event) => {
        console.error("WebSocket error:", event);
        onError?.(event);
      };

      ws.value.onclose = () => {
        isConnected.value = false;
        isConnecting.value = false;
        ws.value = null;

        onDisconnect?.();

        // Attempt to reconnect if enabled
        if (autoReconnect && reconnectAttempts.value < maxReconnectAttempts) {
          reconnectAttempts.value++;
          console.log(`WebSocket disconnected. Reconnecting in ${reconnectInterval}ms... (attempt ${reconnectAttempts.value}/${maxReconnectAttempts})`);
          
          reconnectTimeout.value = setTimeout(() => {
            connect();
          }, reconnectInterval);
        } else if (reconnectAttempts.value >= maxReconnectAttempts) {
          console.error("Max reconnection attempts reached. Please refresh the page.");
        }
      };
    } catch (err) {
      console.error("Failed to create WebSocket connection:", err);
      isConnecting.value = false;
    }
  }

  function disconnect() {
    if (reconnectTimeout.value) {
      clearTimeout(reconnectTimeout.value);
      reconnectTimeout.value = null;
    }

    if (ws.value) {
      ws.value.close();
      ws.value = null;
    }

    isConnected.value = false;
    isConnecting.value = false;
  }

  function send(message: any) {
    if (ws.value && isConnected.value) {
      ws.value.send(typeof message === "string" ? message : JSON.stringify(message));
    } else {
      console.warn("WebSocket is not connected");
    }
  }

  function sendPing() {
    send("ping");
  }

  // Clean up on unmount
  onUnmounted(() => {
    disconnect();
  });

  return {
    connect,
    disconnect,
    send,
    sendPing,
    isConnected,
    isConnecting,
    reconnectAttempts,
  };
}
