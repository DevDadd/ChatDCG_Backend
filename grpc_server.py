import os
from concurrent import futures

import django
import grpc

os.environ.setdefault(
    "DJANGO_SETTINGS_MODULE",
    "config.settings",
)
django.setup()

from chat.models import User
from proto import chat_pb2, chat_pb2_grpc


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


def serve():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10)
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