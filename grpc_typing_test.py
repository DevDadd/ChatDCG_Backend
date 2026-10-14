import sys
import time

import grpc

from proto import chat_pb2, chat_pb2_grpc


def auth_metadata(user_id):
    # The user id returned by the Login RPC
    return (("x-user-id", str(user_id)),)


def subscribe(client, conversation_id, user_id):
    stream = client.SubscribeTyping(
        chat_pb2.SubscribeTypingRequest(
            conversation_id=conversation_id,
            user_id=user_id,
        ),
        metadata=auth_metadata(user_id),
    )

    print(f"Listening for typing in conversation {conversation_id}...")

    try:
        for event in stream:
            status = "is typing..." if event.is_typing else "stopped typing"
            print(f"[{event.timestamp}] {event.display_name} {status}")
    except KeyboardInterrupt:
        stream.cancel()


def send(client, conversation_id, user_id):
    for is_typing in (True, False):
        client.SendTypingStatus(
            chat_pb2.SendTypingStatusRequest(
                conversation_id=conversation_id,
                user_id=user_id,
                is_typing=is_typing,
            ),
            metadata=auth_metadata(user_id),
            timeout=5,
        )
        print(f"Sent is_typing={is_typing}")
        time.sleep(3)


def main():
    if len(sys.argv) != 4 or sys.argv[1] not in ("subscribe", "send"):
        print(
            "Usage: python grpc_typing_test.py "
            "<subscribe|send> <conversation_id> <user_id>"
        )
        sys.exit(1)

    mode = sys.argv[1]
    conversation_id = int(sys.argv[2])
    user_id = int(sys.argv[3])

    with grpc.insecure_channel("localhost:50051") as channel:
        client = chat_pb2_grpc.ChatServiceStub(channel)

        if mode == "subscribe":
            subscribe(client, conversation_id, user_id)
        else:
            send(client, conversation_id, user_id)


if __name__ == "__main__":
    main()
