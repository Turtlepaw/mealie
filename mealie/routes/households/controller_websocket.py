from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect
from pydantic import UUID4

from mealie.core.dependencies.dependencies import get_current_user
from mealie.routes._base.base_controllers import BaseUserController
from mealie.routes._base.controller import controller
from mealie.schema.user.user import PrivateUser
from mealie.services.event_bus_service.websocket_publisher import connection_manager

router = APIRouter(prefix="/ws", tags=["WebSocket"])


@router.websocket("/households/{household_id}")
async def websocket_endpoint(
    websocket: WebSocket,
    household_id: UUID4,
):
    """
    WebSocket endpoint for real-time updates to shopping lists and other household data.
    
    This endpoint provides live updates for:
    - Shopping list creation, updates, and deletion
    - Shopping list item changes (create, update, delete)
    
    Authentication Flow:
    1. Client connects to the WebSocket
    2. Server accepts the connection
    3. Client sends JWT authentication token as the first message
    4. Server validates the token and verifies household access
    5. Server sends connection confirmation
    6. Server begins broadcasting relevant events
    
    Message Format (sent from server):
    ```json
    {
        "type": "connected",  // Connection confirmation
        "household_id": "uuid"
    }
    ```
    
    Or for events:
    ```json
    {
        "event_type": "shopping_list_updated",
        "document_type": "shopping_list",
        "document_data": {...},  // Event-specific data
        "timestamp": "2024-01-01T00:00:00Z",
        "message": "Optional human-readable message"
    }
    ```
    
    Ping/Pong:
    - Client can send "ping" to keep connection alive
    - Server responds with `{"type": "pong"}`
    
    Args:
        websocket: The WebSocket connection
        household_id: UUID of the household to receive updates for
        
    Raises:
        WebSocketDisconnect: When the client disconnects
    """
    from mealie.core.security.providers.credentials_provider import CredentialsProvider
    from mealie.db.db_setup import session_context
    from mealie.repos.all_repositories import get_repositories

    # Accept the connection first
    await websocket.accept()

    try:
        # Wait for authentication token
        token_data = await websocket.receive_text()
        
        # Validate the token
        try:
            with session_context() as session:
                credentials = CredentialsProvider(session=session)
                user = credentials.authenticate_token(token_data)
                
                if not user:
                    await websocket.send_json({"error": "Invalid authentication token"})
                    await websocket.close()
                    return
                
                # Verify user has access to this household
                repos = get_repositories(session, group_id=user.group_id, household_id=user.household_id)
                household = repos.households.get_one(household_id)
                
                if not household or household.group_id != user.group_id:
                    await websocket.send_json({"error": "Access denied to household"})
                    await websocket.close()
                    return
                
                # Register the connection
                await connection_manager.connect(websocket, user.group_id, household_id)
                
                # Send connection confirmation
                await websocket.send_json({"type": "connected", "household_id": str(household_id)})
                
                # Keep the connection alive and handle any incoming messages
                while True:
                    # Receive any message (ping/pong, etc.)
                    data = await websocket.receive_text()
                    
                    # Echo back a pong if we receive a ping
                    if data == "ping":
                        await websocket.send_json({"type": "pong"})
                        
        except Exception as e:
            await websocket.send_json({"error": f"Authentication failed: {str(e)}"})
            await websocket.close()
            return
            
    except WebSocketDisconnect:
        # Client disconnected normally
        pass
    except Exception as e:
        # Log any other errors
        from mealie.core.root_logger import get_logger
        logger = get_logger()
        logger.error(f"WebSocket error: {e}")
    finally:
        # Always clean up the connection
        if 'user' in locals():
            connection_manager.disconnect(websocket, user.group_id, household_id)
