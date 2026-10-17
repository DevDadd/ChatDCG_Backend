import grpc

from proto import chat_pb2, chat_pb2_grpc


def main():
    with grpc.insecure_channel("localhost:50051") as channel:
        client = chat_pb2_grpc.ChatServiceStub(channel)

        # Authenticate as a known user (Register, or Login if it already exists)
        try:
            me = client.Register(
                chat_pb2.RegisterRequest(
                    account="alice_test",
                    password="alice-pass",
                    display_name="Alice",
                ),
                timeout=10,
            ).user
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.ALREADY_EXISTS:
                me = client.Login(
                    chat_pb2.LoginRequest(
                        account="alice_test", password="alice-pass"
                    ),
                    timeout=10,
                ).user
            else:
                raise

        response = client.GetUsersStatus(
            chat_pb2.GetUsersStatusRequest(),
            metadata=(("x-user-id", str(me.id)),),
            timeout=10,
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

        conversations = client.GetConversations(
            chat_pb2.GetConversationsRequest(),
            metadata=(("x-user-id", str(me.id)),),
            timeout=10,
        )

        print("\nConversations:")
        for conversation in conversations.conversations:
            other_user = conversation.other_user
            print(
                f"{conversation.conversation_id} | "
                f"{other_user.username} | "
                f"{other_user.display_name} | "
                f"{conversation.created_at}"
            )


if __name__ == "__main__":
    main()