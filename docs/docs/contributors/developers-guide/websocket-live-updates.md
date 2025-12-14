# WebSocket Live Updates

This document describes the WebSocket implementation for real-time updates in Mealie.

## Overview

Mealie now supports real-time updates for shopping lists via WebSockets. When multiple users are viewing the same shopping list, changes made by one user are immediately reflected for all other users without requiring page refreshes.

## Architecture

### Backend

The WebSocket implementation consists of three main components:

1. **WebSocket Connection Manager** (`mealie/services/event_bus_service/websocket_publisher.py`)
   - Manages active WebSocket connections
   - Organizes connections by group_id and household_id
   - Handles message broadcasting to specific households or entire groups

2. **WebSocket Event Listener** (`mealie/services/event_bus_service/event_bus_listeners.py`)
   - Integrates with the existing event bus
   - Listens for shopping list events (create, update, delete)
   - Broadcasts events to connected WebSocket clients

3. **WebSocket Route** (`mealie/routes/households/controller_websocket.py`)
   - Provides `/api/ws/households/{household_id}` endpoint
   - Handles JWT authentication
   - Manages connection lifecycle (connect, ping/pong, disconnect)

### Frontend

The frontend WebSocket implementation uses composables:

1. **WebSocket Composable** (`frontend/composables/use-websocket.ts`)
   - Generic WebSocket connection management
   - Auto-reconnect with exponential backoff (up to 10 attempts)
   - Ping/pong keep-alive mechanism

2. **Shopping List WebSocket** (`frontend/composables/use-shopping-list-websocket.ts`)
   - Specialized composable for shopping list events
   - Event type filtering and handling

3. **Integration** (`frontend/composables/shopping-list-page/sub-composables/use-shopping-list-data.ts`)
   - Automatically connects to WebSocket when viewing a shopping list
   - Disables polling when WebSocket is connected
   - Falls back to polling if WebSocket fails

## Usage

### For End Users

No configuration is required. Real-time updates are automatically enabled when viewing shopping lists. You'll see changes made by other users instantly.

### For Developers

#### Adding WebSocket Support for New Events

To add WebSocket support for other event types:

1. Update `WebSocketEventListener.get_subscribers()` to include your event type:
```python
if event.event_type in [
    EventTypes.shopping_list_created,
    EventTypes.shopping_list_updated,
    EventTypes.shopping_list_deleted,
    EventTypes.your_new_event,  # Add here
]:
    return [True]
```

2. Create a specialized composable for your feature (optional but recommended):
```typescript
export function useYourFeatureWebSocket(householdId: string) {
  const handleMessage = (message: WebSocketMessage) => {
    if (message.event_type === "your_event_type") {
      // Handle your event
    }
  };

  return useWebSocket(householdId, {
    onMessage: handleMessage,
  });
}
```

3. Integrate with your feature's data management composable

#### Testing WebSocket Connections

Manual testing checklist:
- [ ] Open the same shopping list in two browser windows
- [ ] Make changes in one window (add/edit/delete items)
- [ ] Verify changes appear in the other window within 1-2 seconds
- [ ] Test with poor network conditions (throttle in DevTools)
- [ ] Verify auto-reconnect works after network disruption
- [ ] Verify fallback to polling when WebSocket is unavailable

#### Message Format

Messages sent from the server follow this format:

```json
{
  "event_type": "shopping_list_updated",
  "document_type": "shopping_list",
  "document_data": {
    "operation": "update",
    "shopping_list_id": "uuid"
  },
  "timestamp": "2024-01-01T00:00:00Z",
  "message": "Human-readable message"
}
```

## Security

- WebSocket connections require JWT authentication
- Users can only connect to households they belong to
- All messages are scoped to the user's group and household
- Invalid tokens result in immediate connection closure

## Performance Considerations

- WebSocket connections are lightweight and persistent
- Polling is disabled when WebSocket is connected, reducing server load
- Connection manager automatically cleans up disconnected clients
- Events are broadcast asynchronously to avoid blocking the event bus

## Troubleshooting

### WebSocket Not Connecting

1. Check browser console for errors
2. Verify JWT token is present in cookies
3. Check network tab for WebSocket connection attempts
4. Verify household_id is correct

### Updates Not Appearing

1. Check that WebSocket is connected (console should show "WebSocket authenticated and connected")
2. Verify you're in the same household as other users
3. Check browser console for error messages
4. Verify the event bus is publishing events (backend logs)

### Frequent Disconnections

1. Check for network issues
2. Verify server is running and accessible
3. Check for firewall/proxy issues blocking WebSocket connections
4. Increase reconnection attempts in `use-websocket.ts` if needed

## Future Enhancements

Potential improvements for the WebSocket implementation:

- Support for more event types (recipes, meal plans, etc.)
- Optimistic UI updates with conflict resolution
- Connection status indicators in the UI
- Bandwidth optimization (delta updates instead of full objects)
- Message compression for large payloads
