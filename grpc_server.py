import functools
import os
import queue
import threading
import time
from collections import defaultdict
from concurrent import futures
from contextlib import contextmanager
from datetime import datetime, timezone as dt_timezone

import django
import grpc

os.environ.setdefault(
    "DJANGO_SETTINGS_MODULE",
    "config.settings",
)
django.setup()

from django.db import DatabaseError, IntegrityError, connection, transaction
from django.utils import timezone

from chat.models import Conversation, Message, User
from proto import chat_pb2, chat_pb2_grpc


# Each open stream pins one worker thread for its whole lifetime, so streams
# are capped below the pool size to always leave workers for unary calls.
MAX_STREAMS = int(os.getenv("GRPC_MAX_STREAMS", "200"))
UNARY_WORKERS = int(os.getenv("GRPC_UNARY_WORKERS", "20"))

SUBSCRIBER_QUEUE_SIZE = 1000
MAX_SYNC_BATCH = 1000
CONVERSATION_REFRESH_SECONDS = 10
MAX_FIELD_LENGTH = 100
MAX_BIGINT = 2**63 - 1

# Login marks the user online right away; they stay online past this only if
# the app opens a stream (Presence, Chat, ...) in the meantime.
LOGIN_GRACE_SECONDS = int(os.getenv("GRPC_LOGIN_GRACE_SECONDS", "60"))
PRESENCE_SWEEP_SECONDS = 5
# Every presence event goes out on this one broker key
PRESENCE_TOPIC = 0


class Subscriber:
    """A bounded inbox; flagged as overflowed when the consumer falls behind."""

    def __init__(self, maxsize=SUBSCRIBER_QUEUE_SIZE):
        self.queue = queue.Queue(maxsize)
        self.overflowed = False

    def wake(self):
        try:
            self.queue.put_nowait(None)
        except queue.Full:
            pass


class PubSubBroker:
    """In-memory pub/sub of protobuf events, keyed by conversation_id."""

    def __init__(self):
        self._lock = threading.Lock()
        self._subscribers = defaultdict(set)

    def subscribe(self, conversation_id, subscriber):
        with self._lock:
            self._subscribers[conversation_id].add(subscriber)

    def unsubscribe(self, conversation_id, subscriber):
        with self._lock:
            subs = self._subscribers.get(conversation_id)
            if subs is None:
                return
            subs.discard(subscriber)
            if not subs:
                del self._subscribers[conversation_id]

    def publish(self, conversation_id, event):
        with self._lock:
            subs = list(self._subscribers.get(conversation_id, ()))
        for subscriber in subs:
            try:
                subscriber.queue.put_nowait(event)
            except queue.Full:
                # Drop the slow consumer instead of buffering without limit
                subscriber.overflowed = True
                self.unsubscribe(conversation_id, subscriber)


typing_broker = PubSubBroker()
message_broker = PubSubBroker()
presence_broker = PubSubBroker()

_stream_slots = threading.BoundedSemaphore(MAX_STREAMS)

# Chat streams do their DB work on these few long-lived threads, which keep
# their connections open: no reconnect per message, and no connection held
# per idle stream.
_chat_db = futures.ThreadPoolExecutor(
    max_workers=int(os.getenv("GRPC_CHAT_DB_WORKERS", "4")),
    thread_name_prefix="chat-db",
)


class AbortError(Exception):
    """Raised off the RPC thread; the RPC thread turns it into a status."""

    def __init__(self, code, details):
        super().__init__(details)
        self.code = code
        self.details = details


def releases_db_connection(method):
    # gRPC threads live outside Django's request cycle, so nothing closes
    # their DB connections for us.
    @functools.wraps(method)
    def wrapper(*args, **kwargs):
        try:
            return method(*args, **kwargs)
        finally:
            connection.close()

    return wrapper


def run_chat_db(fn, *args):
    def task():
        try:
            return fn(*args)
        except DatabaseError:
            # Drop a broken connection so the next task reconnects
            connection.close()
            raise

    return _chat_db.submit(task).result()


@contextmanager
def stream_slot(context):
    if not _stream_slots.acquire(blocking=False):
        context.abort(
            grpc.StatusCode.RESOURCE_EXHAUSTED,
            "Too many open streams, retry later",
        )
    try:
        yield
    finally:
        _stream_slots.release()


def save_presence(user_id, is_online, now):
    fields = {"is_online": is_online}
    if not is_online:
        fields["last_seen"] = now
    User.objects.filter(id=user_id).update(**fields)


class PresenceTracker:
    """Who is online: users with an open stream, or a login still in its
    grace period. Writes User.is_online and publishes a PresenceEvent only
    when that changes.
    """

    def __init__(self):
        # Held across the DB write so writes land in the order states changed
        self._lock = threading.Lock()
        # user_id -> contexts of that user's open streams
        self._connections = defaultdict(set)
        self._grace_until = {}
        self._online = set()

    def logged_in(self, user_id):
        with self._lock:
            self._grace_until[user_id] = (
                time.monotonic() + LOGIN_GRACE_SECONDS
            )
            self._sync(user_id)

    def logged_out(self, user_id):
        # Ends every open stream of the user, on all their devices
        with self._lock:
            self._grace_until.pop(user_id, None)
            contexts = self._connections.pop(user_id, set())
            self._sync(user_id)
        for context in contexts:
            context.cancel()

    @contextmanager
    def connection(self, user_id, context):
        with self._lock:
            self._connections[user_id].add(context)
            self._sync(user_id)
        try:
            yield
        finally:
            with self._lock:
                # Already gone if the user logged out meanwhile
                contexts = self._connections.get(user_id)
                if contexts is not None:
                    contexts.discard(context)
                    if not contexts:
                        del self._connections[user_id]
                self._sync(user_id)

    def expire_logins(self):
        now = time.monotonic()
        with self._lock:
            expired = [
                user_id
                for user_id, until in self._grace_until.items()
                if until <= now
            ]
            for user_id in expired:
                del self._grace_until[user_id]
                self._sync(user_id)

    def _sync(self, user_id):
        is_online = (
            user_id in self._connections
            or self._grace_until.get(user_id, 0) > time.monotonic()
        )
        if is_online == (user_id in self._online):
            return

        now = timezone.now()
        try:
            run_chat_db(save_presence, user_id, is_online, now)
        except DatabaseError as exc:
            # Left unchanged so the next change for this user retries
            print(f"Failed to save presence of user {user_id}: {exc}")
            return

        if is_online:
            self._online.add(user_id)
        else:
            self._online.discard(user_id)

        presence_broker.publish(
            PRESENCE_TOPIC,
            chat_pb2.PresenceEvent(
                user_id=user_id,
                is_online=is_online,
                last_seen="" if is_online else now.isoformat(),
            ),
        )


presence = PresenceTracker()


def sweep_expired_logins():
    while True:
        time.sleep(PRESENCE_SWEEP_SECONDS)
        presence.expire_logins()


def authenticate(context):
    """Return the User whose id the client sends as "x-user-id" metadata.

    The client gets this id from the Login/Register response.
    """
    metadata = dict(context.invocation_metadata())
    raw_id = metadata.get("x-user-id", "")
    # isascii(): isdigit() alone accepts characters like "²" that int() rejects
    user_id = (
        int(raw_id) if raw_id.isascii() and raw_id.isdigit() else None
    )
    user = (
        User.objects.filter(id=user_id).first()
        if user_id is not None and user_id <= MAX_BIGINT
        else None
    )
    if user is None:
        context.abort(
            grpc.StatusCode.UNAUTHENTICATED,
            "Missing or unknown x-user-id metadata",
        )
    return user


def check_claimed_user(caller, claimed_id):
    # 0 means "not set": the authenticated caller is used
    if claimed_id and claimed_id != caller.id:
        raise AbortError(
            grpc.StatusCode.PERMISSION_DENIED,
            f"Cannot act as user {claimed_id}",
        )


def get_participant_conversation(caller, conversation_id):
    conversation = (
        Conversation.objects.with_participant(caller)
        .filter(id=conversation_id)
        .first()
    )
    if conversation is None:
        raise AbortError(
            grpc.StatusCode.NOT_FOUND,
            f"Conversation {conversation_id} not found",
        )
    return conversation


@contextmanager
def aborts_on_error(context):
    try:
        yield
    except AbortError as exc:
        context.abort(exc.code, exc.details)


def conversation_ids_of(user):
    return set(
        Conversation.objects.with_participant(user)
        .values_list("id", flat=True)
    )


def save_chat_message(caller, request):
    check_claimed_user(caller, request.sender_id)
    conversation = get_participant_conversation(
        caller, request.conversation_id
    )
    return Message.objects.create(
        conversation=conversation,
        sender=caller,
        content=request.content,
    )


def user_to_pb(user):
    return chat_pb2.UserInfo(
        id=user.id,
        account=user.account,
        username=user.username,
        display_name=user.display_name,
    )


def message_to_pb(message, sender):
    """Serialize a persisted Message into a protobuf ChatMessage."""
    return chat_pb2.ChatMessage(
        message_id=message.id,
        conversation_id=message.conversation_id,
        sender_id=sender.id,
        sender_name=sender.display_name,
        content=message.content,
        timestamp=message.created_at.isoformat(),
    )


def client_sent_at(request, now):
    if request.client_timestamp_ms <= 0:
        return now
    try:
        sent_at = datetime.fromtimestamp(
            request.client_timestamp_ms / 1000,
            tz=dt_timezone.utc,
        )
    except (OverflowError, OSError, ValueError):
        return now
    return min(sent_at, now)


def pump(context, subscriber, skip, is_finished=lambda: False, on_tick=None):
    """Yield events from a subscriber until the RPC ends."""
    while context.is_active() and not is_finished():
        if on_tick is not None:
            on_tick()

        if subscriber.overflowed:
            context.abort(
                grpc.StatusCode.RESOURCE_EXHAUSTED,
                "Client is too slow to keep up, reconnect",
            )

        try:
            event = subscriber.queue.get(timeout=1)
        except queue.Empty:
            continue

        # None is only a wake-up signal
        if event is None or skip(event):
            continue

        yield event


class ChatService(chat_pb2_grpc.ChatServiceServicer):
    @releases_db_connection
    def Register(self, request, context):
        account = request.account.strip()
        # Not stripped: spaces in a password are deliberate
        password = request.password
        username = request.username.strip() or account
        display_name = request.display_name.strip() or account

        if not account or not password:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "account and password are required",
            )

        if any(
            len(value) > MAX_FIELD_LENGTH
            for value in (account, password, username, display_name)
        ):
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"Fields must be at most {MAX_FIELD_LENGTH} characters",
            )

        if User.objects.filter(account=account).exists():
            context.abort(
                grpc.StatusCode.ALREADY_EXISTS,
                "Account already exists",
            )

        if User.objects.filter(username=username).exists():
            context.abort(
                grpc.StatusCode.ALREADY_EXISTS,
                "Username already exists",
            )

        try:
            user = User.objects.create(
                account=account,
                password=password,
                username=username,
                display_name=display_name,
            )
        except IntegrityError:
            # Lost a race with a concurrent registration
            context.abort(
                grpc.StatusCode.ALREADY_EXISTS,
                "Account or username already exists",
            )

        presence.logged_in(user.id)
        return chat_pb2.AuthResponse(user=user_to_pb(user))

    @releases_db_connection
    def Login(self, request, context):
        account = request.account.strip()

        user = (
            User.objects.filter(
                account=account,
                password=request.password,
            ).first()
            if account and request.password
            else None
        )
        if user is None:
            context.abort(
                grpc.StatusCode.UNAUTHENTICATED,
                "Invalid account or password",
            )

        presence.logged_in(user.id)
        return chat_pb2.AuthResponse(user=user_to_pb(user))

    @releases_db_connection
    def Logout(self, request, context):
        caller = authenticate(context)
        presence.logged_out(caller.id)
        return chat_pb2.LogoutResponse(success=True)

    @releases_db_connection
    def GetUsersStatus(self, request, context):
        caller = authenticate(context)
        
        response = chat_pb2.GetUsersStatusResponse()

        users = User.objects.exclude(id=caller.id).order_by("id")

        for user in users:
            response.users.add(
                user_id=user.id,
                username=user.username,
                display_name=user.display_name,
                is_online=user.is_online,
                last_seen=(
                    user.last_seen.isoformat()
                    if user.last_seen
                    else ""
                ),
            )

        return response

    @releases_db_connection
    def SendTypingStatus(self, request, context):
        caller = authenticate(context)

        with aborts_on_error(context):
            check_claimed_user(caller, request.user_id)
            get_participant_conversation(caller, request.conversation_id)

        typing_broker.publish(
            request.conversation_id,
            chat_pb2.TypingEvent(
                conversation_id=request.conversation_id,
                user_id=caller.id,
                username=caller.username,
                display_name=caller.display_name,
                is_typing=request.is_typing,
                timestamp=timezone.now().isoformat(),
            ),
        )

        return chat_pb2.SendTypingStatusResponse(success=True)

    def SubscribeTyping(self, request, context):
        with stream_slot(context):
            try:
                caller = authenticate(context)
                with aborts_on_error(context):
                    check_claimed_user(caller, request.user_id)
                    get_participant_conversation(
                        caller, request.conversation_id
                    )
            finally:
                # Don't hold a DB connection for the life of the stream
                connection.close()

            subscriber = Subscriber()
            typing_broker.subscribe(request.conversation_id, subscriber)

            try:
                with presence.connection(caller.id, context):
                    # Don't echo the subscriber's own typing back to them
                    yield from pump(
                        context,
                        subscriber,
                        skip=lambda event: event.user_id == caller.id,
                    )
            finally:
                typing_broker.unsubscribe(request.conversation_id, subscriber)

    @releases_db_connection
    def SendMessage(self, request, context):
        caller = authenticate(context)

        with aborts_on_error(context):
            check_claimed_user(caller, request.sender_id)
            conversation = get_participant_conversation(
                caller, request.conversation_id
            )

        message = Message.objects.create(
            conversation=conversation,
            sender=caller,
            content=request.content,
        )

        message_broker.publish(conversation.id, message_to_pb(message, caller))

        return chat_pb2.SendMessageResponse(
            success=True,
            message="Message sent",
            message_id=message.id,
        )

    def StreamMessages(self, request, context):
        with stream_slot(context):
            try:
                caller = authenticate(context)
                with aborts_on_error(context):
                    check_claimed_user(caller, request.user_id)
                    get_participant_conversation(
                        caller, request.conversation_id
                    )
            finally:
                connection.close()

            subscriber = Subscriber()
            message_broker.subscribe(request.conversation_id, subscriber)

            try:
                with presence.connection(caller.id, context):
                    # Don't echo the subscriber's own messages back to them
                    yield from pump(
                        context,
                        subscriber,
                        skip=lambda event: event.sender_id == caller.id,
                    )
            finally:
                message_broker.unsubscribe(
                    request.conversation_id, subscriber
                )

    @releases_db_connection
    def SyncMessages(self, request_iterator, context):
        # Client streaming: persist a batch of messages written offline in one
        # transaction, then publish them so online participants see them.
        caller = authenticate(context)

        requests = []
        for request in request_iterator:
            if len(requests) >= MAX_SYNC_BATCH:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    f"At most {MAX_SYNC_BATCH} messages per sync",
                )
            requests.append(request)

        allowed_conversation_ids = set(
            Conversation.objects.with_participant(caller)
            .filter(id__in={request.conversation_id for request in requests})
            .values_list("id", flat=True)
        )

        now = timezone.now()
        messages = []
        failed_count = 0

        for request in requests:
            if (
                request.sender_id and request.sender_id != caller.id
            ) or request.conversation_id not in allowed_conversation_ids:
                failed_count += 1
                continue

            messages.append(
                Message(
                    conversation_id=request.conversation_id,
                    sender=caller,
                    content=request.content,
                    created_at=client_sent_at(request, now),
                )
            )

        with transaction.atomic():
            created = Message.objects.bulk_create(messages)

        for message in sorted(created, key=lambda m: m.created_at):
            message_broker.publish(
                message.conversation_id, message_to_pb(message, caller)
            )

        received_count = len(created)
        return chat_pb2.SyncMessagesResponse(
            received_count=received_count,
            failed_count=failed_count,
            message=(
                f"Synced {received_count} message(s), "
                f"failed {failed_count}"
            ),
        )

    def Chat(self, request_iterator, context):
        # Bidirectional streaming: a background thread reads the client's
        # outgoing messages (persisting + publishing them), while this loop
        # streams messages from *other* participants back to the client.
        with stream_slot(context):
            try:
                caller = authenticate(context)
            finally:
                connection.close()

            # One inbox for every conversation of the caller
            subscriber = Subscriber()
            subscribed = set()
            subscriptions_lock = threading.Lock()
            closed = False
            reader_state = {"error": None}
            last_refresh = time.monotonic()

            def subscribe_new(conversation_ids):
                with subscriptions_lock:
                    if closed:
                        return
                    for conversation_id in conversation_ids - subscribed:
                        message_broker.subscribe(conversation_id, subscriber)
                        subscribed.add(conversation_id)

            def refresh_conversations():
                # Picks up conversations others created after the stream
                # opened
                nonlocal last_refresh
                now = time.monotonic()
                if now - last_refresh < CONVERSATION_REFRESH_SECONDS:
                    return
                last_refresh = now
                try:
                    subscribe_new(
                        run_chat_db(conversation_ids_of, caller)
                    )
                except DatabaseError:
                    pass  # Keep streaming; retried on the next refresh

            # Subscribe before reading anything so no message is missed
            subscribe_new(run_chat_db(conversation_ids_of, caller))

            def reader():
                try:
                    for request in request_iterator:
                        message = run_chat_db(
                            save_chat_message, caller, request
                        )
                        subscribe_new({message.conversation_id})
                        message_broker.publish(
                            message.conversation_id,
                            message_to_pb(message, caller),
                        )
                except Exception as exc:
                    reader_state["error"] = exc
                    subscriber.wake()

            threading.Thread(target=reader, daemon=True).start()

            try:
                with presence.connection(caller.id, context):
                    # The client closing its send side doesn't end the stream:
                    # it may still be listening.
                    yield from pump(
                        context,
                        subscriber,
                        skip=lambda event: event.sender_id == caller.id,
                        is_finished=lambda: reader_state["error"] is not None,
                        on_tick=refresh_conversations,
                    )

                    error = reader_state["error"]
                    if isinstance(error, AbortError):
                        context.abort(error.code, error.details)
                    elif error is not None and context.is_active():
                        context.abort(
                            grpc.StatusCode.INTERNAL,
                            "Failed to process message",
                        )
            finally:
                with subscriptions_lock:
                    closed = True
                    for conversation_id in subscribed:
                        message_broker.unsubscribe(conversation_id, subscriber)

    def Presence(self, request, context):
        with stream_slot(context):
            try:
                caller = authenticate(context)
            finally:
                connection.close()

            subscriber = Subscriber()
            presence_broker.subscribe(PRESENCE_TOPIC, subscriber)

            try:
                with presence.connection(caller.id, context):
                    yield from pump(
                        context,
                        subscriber,
                        skip=lambda event: event.user_id == caller.id,
                    )
            finally:
                presence_broker.unsubscribe(PRESENCE_TOPIC, subscriber)


def serve():
    # Presence is only as fresh as the server's view of the connection:
    # these pings drop a client whose network vanished without closing it.
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=MAX_STREAMS + UNARY_WORKERS),
        options=[
            ("grpc.keepalive_time_ms", 30_000),
            ("grpc.keepalive_timeout_ms", 10_000),
            ("grpc.keepalive_permit_without_calls", 1),
            ("grpc.http2.min_ping_interval_without_data_ms", 10_000),
            ("grpc.http2.max_pings_without_data", 0),
        ],
    )

    # Nobody is connected yet: clear whoever was online when we last stopped
    User.objects.filter(is_online=True).update(
        is_online=False, last_seen=timezone.now()
    )
    connection.close()
    threading.Thread(target=sweep_expired_logins, daemon=True).start()

    chat_pb2_grpc.add_ChatServiceServicer_to_server(
        ChatService(),
        server,
    )

    listen_port = os.getenv("PORT", "50051")
    port = server.add_insecure_port(f"0.0.0.0:{listen_port}")

    if port == 0:
        raise RuntimeError(f"Cannot bind to port {listen_port}")

    server.start()
    print(f"gRPC server running on port {port}")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=5).wait()


if __name__ == "__main__":
    serve()
