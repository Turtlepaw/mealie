from typing import Any

from fastapi import WebSocket
from pydantic import UUID4

from mealie.core.root_logger import get_logger

logger = get_logger()


class DummyPublisher:
    """
    Dummy publisher for WebSocket event listener.
    
    Since WebSocket broadcasting happens via the connection manager rather than
    a traditional publisher pattern, this class provides a no-op implementation
    to satisfy the event listener interface.
    """

    def publish(self, event: Any, subscribers: list) -> None:
        """No-op publisher method"""
        pass


class WebSocketConnectionManager:
    """
    Manages WebSocket connections for real-time updates across the application.
    
    This connection manager organizes connections by group and household,
    allowing targeted broadcasting of events to specific sets of users.
    
    The connection structure is:
    - Group ID -> Household ID -> List of WebSocket connections
    
    This allows efficient message routing:
    - Send to all users in a household
    - Broadcast to all households in a group
    """

    def __init__(self):
        # Map of group_id -> household_id -> list of WebSocket connections
        self.active_connections: dict[UUID4, dict[UUID4, list[WebSocket]]] = {}

    async def connect(self, websocket: WebSocket, group_id: UUID4, household_id: UUID4) -> None:
        """
        Accept and register a new WebSocket connection.
        
        Args:
            websocket: The WebSocket connection to register
            group_id: ID of the group the connection belongs to
            household_id: ID of the household the connection belongs to
        """
        await websocket.accept()

        if group_id not in self.active_connections:
            self.active_connections[group_id] = {}

        if household_id not in self.active_connections[group_id]:
            self.active_connections[group_id][household_id] = []

        self.active_connections[group_id][household_id].append(websocket)
        logger.debug(f"WebSocket connected: group={group_id}, household={household_id}")

    def disconnect(self, websocket: WebSocket, group_id: UUID4, household_id: UUID4) -> None:
        """
        Remove a WebSocket connection and clean up empty structures.
        
        Args:
            websocket: The WebSocket connection to remove
            group_id: ID of the group the connection belongs to
            household_id: ID of the household the connection belongs to
        """
        if (
            group_id in self.active_connections
            and household_id in self.active_connections[group_id]
            and websocket in self.active_connections[group_id][household_id]
        ):
            self.active_connections[group_id][household_id].remove(websocket)

            # Clean up empty dictionaries
            if not self.active_connections[group_id][household_id]:
                del self.active_connections[group_id][household_id]

            if not self.active_connections[group_id]:
                del self.active_connections[group_id]

            logger.debug(f"WebSocket disconnected: group={group_id}, household={household_id}")

    async def send_to_household(self, message: dict[str, Any], group_id: UUID4, household_id: UUID4) -> None:
        """
        Send a message to all connections in a specific household.
        
        This method handles connection failures gracefully by removing
        disconnected clients automatically.
        
        Args:
            message: The message to send (will be JSON-encoded)
            group_id: ID of the group
            household_id: ID of the household to send to
        """
        if group_id not in self.active_connections or household_id not in self.active_connections[group_id]:
            return

        connections = self.active_connections[group_id][household_id].copy()
        disconnected = []

        for connection in connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.warning(f"Failed to send WebSocket message: {e}")
                disconnected.append(connection)

        # Clean up disconnected connections
        for connection in disconnected:
            self.disconnect(connection, group_id, household_id)

    async def broadcast_to_group(self, message: dict[str, Any], group_id: UUID4) -> None:
        """
        Send a message to all connections in a group (all households).
        
        Args:
            message: The message to send (will be JSON-encoded)
            group_id: ID of the group to broadcast to
        """
        if group_id not in self.active_connections:
            return

        for household_id in list(self.active_connections[group_id].keys()):
            await self.send_to_household(message, group_id, household_id)


# Global connection manager instance
connection_manager = WebSocketConnectionManager()
