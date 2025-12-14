import pytest
from pydantic import UUID4

from mealie.services.event_bus_service.websocket_publisher import WebSocketConnectionManager


class MockWebSocket:
    """Mock WebSocket for testing"""

    def __init__(self):
        self.accepted = False
        self.messages_sent = []
        self.closed = False

    async def accept(self):
        self.accepted = True

    async def send_json(self, message):
        self.messages_sent.append(message)

    async def close(self):
        self.closed = True


def test_connection_manager_initialization():
    """Test that connection manager initializes correctly"""
    manager = WebSocketConnectionManager()
    assert manager.active_connections == {}


@pytest.mark.asyncio
async def test_connection_manager_connect():
    """Test that connections are registered correctly"""
    manager = WebSocketConnectionManager()
    ws = MockWebSocket()
    group_id = UUID4("00000000-0000-0000-0000-000000000001")
    household_id = UUID4("00000000-0000-0000-0000-000000000002")

    await manager.connect(ws, group_id, household_id)

    assert ws.accepted is True
    assert group_id in manager.active_connections
    assert household_id in manager.active_connections[group_id]
    assert ws in manager.active_connections[group_id][household_id]


@pytest.mark.asyncio
async def test_connection_manager_disconnect():
    """Test that connections are removed correctly"""
    manager = WebSocketConnectionManager()
    ws = MockWebSocket()
    group_id = UUID4("00000000-0000-0000-0000-000000000001")
    household_id = UUID4("00000000-0000-0000-0000-000000000002")

    await manager.connect(ws, group_id, household_id)
    manager.disconnect(ws, group_id, household_id)

    assert group_id not in manager.active_connections


@pytest.mark.asyncio
async def test_connection_manager_send_to_household():
    """Test sending messages to all connections in a household"""
    manager = WebSocketConnectionManager()
    ws1 = MockWebSocket()
    ws2 = MockWebSocket()
    group_id = UUID4("00000000-0000-0000-0000-000000000001")
    household_id = UUID4("00000000-0000-0000-0000-000000000002")

    await manager.connect(ws1, group_id, household_id)
    await manager.connect(ws2, group_id, household_id)

    test_message = {"type": "test", "data": "hello"}
    await manager.send_to_household(test_message, group_id, household_id)

    assert len(ws1.messages_sent) == 1
    assert ws1.messages_sent[0] == test_message
    assert len(ws2.messages_sent) == 1
    assert ws2.messages_sent[0] == test_message


@pytest.mark.asyncio
async def test_connection_manager_broadcast_to_group():
    """Test broadcasting messages to all households in a group"""
    manager = WebSocketConnectionManager()
    ws1 = MockWebSocket()
    ws2 = MockWebSocket()
    group_id = UUID4("00000000-0000-0000-0000-000000000001")
    household_id1 = UUID4("00000000-0000-0000-0000-000000000002")
    household_id2 = UUID4("00000000-0000-0000-0000-000000000003")

    await manager.connect(ws1, group_id, household_id1)
    await manager.connect(ws2, group_id, household_id2)

    test_message = {"type": "test", "data": "broadcast"}
    await manager.broadcast_to_group(test_message, group_id)

    assert len(ws1.messages_sent) == 1
    assert ws1.messages_sent[0] == test_message
    assert len(ws2.messages_sent) == 1
    assert ws2.messages_sent[0] == test_message


@pytest.mark.asyncio
async def test_connection_manager_multiple_connections_per_household():
    """Test that multiple connections can exist for the same household"""
    manager = WebSocketConnectionManager()
    ws1 = MockWebSocket()
    ws2 = MockWebSocket()
    ws3 = MockWebSocket()
    group_id = UUID4("00000000-0000-0000-0000-000000000001")
    household_id = UUID4("00000000-0000-0000-0000-000000000002")

    await manager.connect(ws1, group_id, household_id)
    await manager.connect(ws2, group_id, household_id)
    await manager.connect(ws3, group_id, household_id)

    assert len(manager.active_connections[group_id][household_id]) == 3

    # Disconnect one and verify the others remain
    manager.disconnect(ws2, group_id, household_id)
    assert len(manager.active_connections[group_id][household_id]) == 2
    assert ws1 in manager.active_connections[group_id][household_id]
    assert ws3 in manager.active_connections[group_id][household_id]
    assert ws2 not in manager.active_connections[group_id][household_id]
