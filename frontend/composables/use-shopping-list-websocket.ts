import { useWebSocket } from "./use-websocket";
import type { WebSocketMessage } from "./use-websocket";

export interface ShoppingListWebSocketHandlers {
  onShoppingListCreated?: (data: any) => void;
  onShoppingListUpdated?: (data: any) => void;
  onShoppingListDeleted?: (data: any) => void;
}

export function useShoppingListWebSocket(
  householdId: string,
  handlers: ShoppingListWebSocketHandlers = {}
) {
  const { onShoppingListCreated, onShoppingListUpdated, onShoppingListDeleted } = handlers;

  const handleMessage = (message: WebSocketMessage) => {
    // Only process shopping list events
    if (message.document_type !== "shopping_list" && message.document_type !== "shopping_list_item") {
      return;
    }

    switch (message.event_type) {
      case "shopping_list_created":
        onShoppingListCreated?.(message.document_data);
        break;
      case "shopping_list_updated":
        onShoppingListUpdated?.(message.document_data);
        break;
      case "shopping_list_deleted":
        onShoppingListDeleted?.(message.document_data);
        break;
    }
  };

  const websocket = useWebSocket(householdId, {
    onMessage: handleMessage,
    onError: (error) => {
      console.error("Shopping list WebSocket error:", error);
    },
    onConnect: () => {
      console.log("Shopping list WebSocket connected");
    },
    onDisconnect: () => {
      console.log("Shopping list WebSocket disconnected");
    },
  });

  return websocket;
}
