import grpc

from proto import chat_pb2, chat_pb2_grpc


def main():
    with grpc.insecure_channel("localhost:50051") as channel:
        client = chat_pb2_grpc.ChatServiceStub(channel)

        response = client.GetUsersStatus(
            chat_pb2.GetUsersStatusRequest(),
            timeout=5,
        )

        for user in response.users:
            status = "ONLINE" if user.is_online else "OFFLINE"

            print(
                f"{user.user_id} | "
                f"{user.username} | "
                f"{user.display_name} | "
                f"{status} | "
                f"{user.last_seen or 'Never'}"
            )


if __name__ == "__main__":
    main()