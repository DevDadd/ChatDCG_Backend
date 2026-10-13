import os
import queue
import threading
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

from chat.models import User
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