import contextlib
import json
from abc import ABC, abstractmethod
from collections.abc import Generator
from datetime import UTC, datetime
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

from fastapi.encoders import jsonable_encoder
from pydantic import UUID4
from sqlalchemy import select
from sqlalchemy.orm.session import Session

from mealie.db.db_setup import session_context
from mealie.db.models.household.webhooks import GroupWebhooksModel
from mealie.repos.repository_factory import AllRepositories
from mealie.schema.household.group_events import GroupEventNotifierPrivate
from mealie.schema.household.webhook import ReadWebhook

from .event_types import Event, EventDocumentType, EventTypes, EventWebhookData
from .publisher import ApprisePublisher, PublisherLike, WebhookPublisher
from .websocket_publisher import connection_manager


class EventListenerBase(ABC):
    _session: Session | None = None
    _repos: AllRepositories | None = None

    def __init__(self, group_id: UUID4, household_id: UUID4, publisher: PublisherLike) -> None:
        self.group_id = group_id
        self.household_id = household_id
        self.publisher = publisher
        self._session = None
        self._repos = None

    @abstractmethod
    def get_subscribers(self, event: Event) -> list:
        """Get a list of all subscribers to this event"""
        ...

    @abstractmethod
    def publish_to_subscribers(self, event: Event, subscribers: list) -> None:
        """Publishes the event to all subscribers"""
        ...

    @contextlib.contextmanager
    def ensure_session(self) -> Generator[Session, None, None]:
        """
        ensure_session ensures that a session is available for the caller by checking if a session
        was provided during construction, and if not, creating a new session with the `with_session`
        function and closing it when the context manager exits.

        This is _required_ when working with sessions inside an event bus listener where the listener
        may be constructed during a request where the session is provided by the request, but the when
        run as a scheduled task, the session is not provided and must be created.
        """
        if self._session is None:
            with session_context() as session:
                self._session = session
                yield self._session
        else:
            yield self._session

    @contextlib.contextmanager
    def ensure_repos(self, group_id: UUID4, household_id: UUID4) -> Generator[AllRepositories, None, None]:
        if self._repos is None:
            with self.ensure_session() as session:
                self._repos = AllRepositories(session, group_id=group_id, household_id=household_id)
                yield self._repos
        else:
            yield self._repos


class AppriseEventListener(EventListenerBase):
    def __init__(self, group_id: UUID4, household_id: UUID4) -> None:
        super().__init__(group_id, household_id, ApprisePublisher())

    def get_subscribers(self, event: Event) -> list[str]:
        with self.ensure_repos(self.group_id, self.household_id) as repos:
            notifiers: list[GroupEventNotifierPrivate] = repos.group_event_notifier.multi_query(
                {"enabled": True}, override_schema=GroupEventNotifierPrivate
            )

            urls = [notifier.apprise_url for notifier in notifiers if getattr(notifier.options, event.event_type.name)]
            urls = AppriseEventListener.update_urls_with_event_data(urls, event)

        return urls

    def publish_to_subscribers(self, event: Event, subscribers: list[str]) -> None:
        self.publisher.publish(event, subscribers)

    @staticmethod
    def update_urls_with_event_data(urls: list[str], event: Event):
        params = {
            "event_type": event.event_type.name,
            "integration_id": event.integration_id,
            "document_data": json.dumps(jsonable_encoder(event.document_data)),
            "event_id": str(event.event_id),
            "timestamp": event.timestamp.isoformat() if event.timestamp else None,
        }

        return [
            # We use query params to add custom key: value pairs to the Apprise payload by prepending the key with ":".
            (
                AppriseEventListener.merge_query_parameters(url, {f":{k}": v for k, v in params.items()})
                # only certain endpoints support the custom key: value pairs, so we only apply them to those endpoints
                if AppriseEventListener.is_custom_url(url)
                else url
            )
            for url in urls
        ]

    @staticmethod
    def merge_query_parameters(url: str, params: dict):
        scheme, netloc, path, query_string, fragment = urlsplit(url)

        # merge query params
        query_params = parse_qs(query_string)
        query_params.update(params)
        new_query_string = urlencode(query_params, doseq=True)

        return urlunsplit((scheme, netloc, path, new_query_string, fragment))

    @staticmethod
    def is_custom_url(url: str):
        return url.split(":", 1)[0].lower() in [
            "form",
            "forms",
            "json",
            "jsons",
            "xml",
            "xmls",
        ]


class WebhookEventListener(EventListenerBase):
    def __init__(self, group_id: UUID4, household_id: UUID4) -> None:
        super().__init__(group_id, household_id, WebhookPublisher())

    def get_subscribers(self, event: Event) -> list[ReadWebhook]:
        # we only care about events that contain webhook information
        if not (event.event_type == EventTypes.webhook_task and isinstance(event.document_data, EventWebhookData)):
            return []

        scheduled_webhooks = self.get_scheduled_webhooks(
            event.document_data.webhook_start_dt, event.document_data.webhook_end_dt
        )
        return scheduled_webhooks

    def publish_to_subscribers(self, event: Event, subscribers: list[ReadWebhook]) -> None:
        with self.ensure_repos(self.group_id, self.household_id) as repos:
            if not isinstance(event.document_data, EventWebhookData):
                return

            match event.document_data.document_type:
                case EventDocumentType.mealplan:
                    meal_repo = repos.meals
                    meal_data = meal_repo.get_meals_by_date_range(
                        event.document_data.webhook_start_dt, event.document_data.webhook_end_dt
                    )
                    event.document_data.webhook_body = meal_data or None
                case _:
                    if event.event_type is EventTypes.test_message:
                        # make sure the webhook has a valid body so it gets sent
                        event.document_data.webhook_body = event.document_data.webhook_body or []

            # Only publish to subscribers if we have a webhook body to send
            if event.document_data.webhook_body is not None:
                self.publisher.publish(event, [webhook.url for webhook in subscribers])

    def get_scheduled_webhooks(self, start_dt: datetime, end_dt: datetime) -> list[ReadWebhook]:
        """Fetches all scheduled webhooks from the database"""
        with self.ensure_session() as session:
            stmt = select(GroupWebhooksModel).where(
                GroupWebhooksModel.enabled == True,  # noqa: E712 - required for SQLAlchemy comparison
                GroupWebhooksModel.scheduled_time > start_dt.astimezone(UTC).time(),
                GroupWebhooksModel.scheduled_time <= end_dt.astimezone(UTC).time(),
                GroupWebhooksModel.group_id == self.group_id,
                GroupWebhooksModel.household_id == self.household_id,
            )
            return session.execute(stmt).scalars().all()


class WebSocketEventListener(EventListenerBase):
    """
    Event bus listener for broadcasting events to WebSocket clients.
    
    This listener integrates with the existing event bus to provide real-time
    updates to connected WebSocket clients. It currently supports shopping list
    events but can be extended to support other event types.
    
    The listener filters events by type and broadcasts them to all WebSocket
    connections in the affected household. Broadcasting is done asynchronously
    to avoid blocking the event bus.
    """

    def __init__(self, group_id: UUID4, household_id: UUID4) -> None:
        from .websocket_publisher import DummyPublisher

        super().__init__(group_id, household_id, DummyPublisher())

    def get_subscribers(self, event: Event) -> list:
        """
        Determine if this event should be broadcast via WebSocket.
        
        Currently supports:
        - shopping_list_created
        - shopping_list_updated
        - shopping_list_deleted
        
        Args:
            event: The event to check
            
        Returns:
            List with one element if event should be broadcast, empty list otherwise
        """
        # Only process shopping list related events
        if event.event_type in [
            EventTypes.shopping_list_created,
            EventTypes.shopping_list_updated,
            EventTypes.shopping_list_deleted,
        ]:
            return [True]  # Return non-empty list to trigger publish_to_subscribers
        return []

    def publish_to_subscribers(self, event: Event, subscribers: list) -> None:
        """
        Broadcast event to all connected WebSocket clients in the household.
        
        The event is serialized to JSON and sent asynchronously to avoid blocking
        the event bus. If there's no event loop (e.g., in tests), broadcasting
        is skipped gracefully.
        
        Args:
            event: The event to broadcast
            subscribers: List of subscribers (unused for WebSocket)
        """
        import asyncio

        from fastapi.encoders import jsonable_encoder

        message = {
            "event_type": event.event_type.name,
            "document_type": event.document_data.document_type.value if event.document_data else None,
            "document_data": jsonable_encoder(event.document_data) if event.document_data else None,
            "timestamp": event.timestamp.isoformat() if event.timestamp else None,
            "message": event.message.body if event.message else None,
        }

        # Create a task to send the message
        # We use create_task to avoid blocking the event bus
        try:
            asyncio.create_task(connection_manager.send_to_household(message, self.group_id, self.household_id))
        except RuntimeError:
            # If there's no event loop, we're probably in a test or non-async context
            # In this case, we can skip WebSocket broadcasting
            pass
