import os
import queue
import threading
import time
from collections import defaultdict
from concurrent import futures

import django
import grpc
from django.utils import timezone

os.environ.setdefault(
    "DJANGO_SETTINGS_MODULE",
    "config.settings",
)
django.setup()

from chat.models import Conversation, Message, User
from proto import chat_pb2, chat_pb2_grpc


class TypingBroker:
    def __init__(self):
        self._lock = threading.Lock()
        self._subscribers = defaultdict(set)

    def subscribe(self, conversation_id):
        q = queue.Queue()
        with self._lock:
            self._subscribers[conversation_id].add(q)
        return q

    def unsubscribe(self, conversation_id, q):
        with self._lock:
            subs = self._subscribers.get(conversation_id)
            if subs is None:
                return
            subs.discard(q)
            if not subs:
                del self._subscribers[conversation_id]

    def publish(self, conversation_id, event):
        with self._lock:
            subs = list(self._subscribers.get(conversation_id, ()))
        for q in subs:
            q.put(event)


typing_broker = TypingBroker()


class MessageBroker:
    """Fan out new chat messages to every subscriber of a conversation."""

    def __init__(self):
        self._lock = threading.Lock()
        self._subscribers = defaultdict(set)

    def subscribe(self, conversation_id):
        q = queue.Queue()
        with self._lock:
            self._subscribers[conversation_id].add(q)
        return q

    def unsubscribe(self, conversation_id, q):
        with self._lock:
            subs = self._subscribers.get(conversation_id)
            if subs is None:
                return
            subs.discard(q)
            if not subs:
                del self._subscribers[conversation_id]

    def publish(self, conversation_id, message):
        with self._lock:
            subs = list(self._subscribers.get(conversation_id, ()))
        for q in subs:
            q.put(message)


message_broker = MessageBroker()


def message_to_pb(message):
    """Serialize a persisted Message into a protobuf ChatMessage."""
    return chat_pb2.ChatMessage(
        message_id=message.id,
        conversation_id=message.conversation_id,
        sender_id=message.sender_id,
        sender_name=message.sender.display_name,
        content=message.content,
        timestamp=message.created_at.isoformat(),
    )


class ChatService(chat_pb2_grpc.ChatServiceServicer):
    def GetUsersStatus(self, request, context):
        response = chat_pb2.GetUsersStatusResponse()

        users = User.objects.all().order_by("id")

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

    def SendTypingStatus(self, request, context):
        user = User.objects.filter(id=request.user_id).first()

        if user is None:
            context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"User {request.user_id} not found",
            )

        typing_broker.publish(
            request.conversation_id,
            chat_pb2.TypingEvent(
                conversation_id=request.conversation_id,
                user_id=user.id,
                username=user.username,
                display_name=user.display_name,
                is_typing=request.is_typing,
                timestamp=timezone.now().isoformat(),
            ),
        )

        return chat_pb2.SendTypingStatusResponse(success=True)

    def SubscribeTyping(self, request, context):
        q = typing_broker.subscribe(request.conversation_id)

        try:
            while context.is_active():
                try:
                    event = q.get(timeout=1)
                except queue.Empty:
                    continue

                # Don't echo the subscriber's own typing back to them
                if event.user_id == request.user_id:
                    continue

                yield event
        finally:
            typing_broker.unsubscribe(request.conversation_id, q)


    def SendMessage(self, request, context):
        # Unary: validate, persist, and fan the message out to subscribers.
        sender = User.objects.filter(id=request.sender_id).first()
        if sender is None:
            context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"User {request.sender_id} not found",
            )

        conversation = Conversation.objects.filter(
            id=request.conversation_id
        ).first()
        if conversation is None:
            context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"Conversation {request.conversation_id} not found",
            )

        message = Message.objects.create(
            conversation=conversation,
            sender=sender,
            content=request.content,
        )

        message_broker.publish(conversation.id, message_to_pb(message))

        return chat_pb2.SendMessageResponse(
            success=True,
            message="Message sent",
            message_id=message.id,
        )

    def StreamMessages(self, request, context):
        # Server streaming: push new messages of a conversation to the client.
        q = message_broker.subscribe(request.conversation_id)

        try:
            while context.is_active():
                try:
                    event = q.get(timeout=1)
                except queue.Empty:
                    continue

                # Don't echo the subscriber's own messages back to them
                if event.sender_id == request.user_id:
                    continue

                yield event
        finally:
            message_broker.unsubscribe(request.conversation_id, q)

    def SyncMessages(self, request_iterator, context):
        # Client streaming: accept a batch of pending (offline) messages,
        # persist each, and return an aggregate summary once the client closes
        # the stream. These are historical messages, so we deliberately do NOT
        # publish them to live subscribers (that would re-broadcast old
        # messages as if they were new). Live sending uses SendMessage / Chat.
        received_count = 0
        failed_count = 0

        for request in request_iterator:
            sender = User.objects.filter(id=request.sender_id).first()
            conversation = Conversation.objects.filter(
                id=request.conversation_id
            ).first()

            if sender is None or conversation is None:
                failed_count += 1
                continue

            Message.objects.create(
                conversation=conversation,
                sender=sender,
                content=request.content,
            )
            received_count += 1

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
        control = queue.Queue()
        subscriptions = {}  # conversation_id -> (broker_queue, owner_user_id)

        def reader():
            try:
                for request in request_iterator:
                    sender = User.objects.filter(
                        id=request.sender_id
                    ).first()
                    conversation = Conversation.objects.filter(
                        id=request.conversation_id
                    ).first()

                    if sender is None or conversation is None:
                        continue

                    control.put(
                        ("subscribe", conversation.id, request.sender_id)
                    )

                    message = Message.objects.create(
                        conversation=conversation,
                        sender=sender,
                        content=request.content,
                    )
                    message_broker.publish(
                        conversation.id, message_to_pb(message)
                    )
            finally:
                control.put(("done", 0, 0))

        threading.Thread(target=reader, daemon=True).start()

        try:
            while context.is_active():
                # Handle subscribe / done signals from the reader thread.
                while True:
                    try:
                        kind, conv_id, owner_id = control.get_nowait()
                    except queue.Empty:
                        break
                    if kind == "subscribe" and conv_id not in subscriptions:
                        subscriptions[conv_id] = (
                            message_broker.subscribe(conv_id),
                            owner_id,
                        )

                # Yield any messages other participants have sent.
                produced = False
                for conv_id, (bq, owner_id) in list(subscriptions.items()):
                    while True:
                        try:
                            event = bq.get_nowait()
                        except queue.Empty:
                            break
                        if event.sender_id == owner_id:
                            continue
                        yield event
                        produced = True

                if not produced:
                    time.sleep(0.1)
        finally:
            for conv_id, (bq, _owner_id) in subscriptions.items():
                message_broker.unsubscribe(conv_id, bq)


def serve():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=50)
    )

    chat_pb2_grpc.add_ChatServiceServicer_to_server(
        ChatService(),
        server,
    )

    port = server.add_insecure_port("[::]:50051")

    if port == 0:
        raise RuntimeError("Cannot bind to port 50051")

    server.start()
    print(f"gRPC server running on port {port}")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=5).wait()


if __name__ == "__main__":
    serve()