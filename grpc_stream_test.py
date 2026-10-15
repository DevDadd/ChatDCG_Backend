# End-to-end test for the deployed ChatService.
# Covers the 4 gRPC types: unary (SendMessage), server stream (StreamMessages),
# client stream (SyncMessages), bidirectional (Chat), plus Register/Login.
# Local:   python grpc_stream_test.py
# Railway: set GRPC_TEST_TARGET=iriguchi.proxy.rlwy.net:51141  then run
import os
import sys
import threading
import time

import django
import grpc

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from chat.models import Conversation
from proto import chat_pb2, chat_pb2_grpc

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

TARGET = os.getenv('GRPC_TEST_TARGET', 'localhost:50051')
TIMEOUT = float(os.getenv('GRPC_TEST_TIMEOUT', '10'))
SKIP_DB_SEED = os.getenv('SKIP_DB_SEED', '') == '1'
FIXED_CONVERSATION_ID = os.getenv('CONVERSATION_ID', '')


def meta(user_id):
    return (('x-user-id', str(user_id)),)


def register_or_login(stub, account, password, display_name):
    try:
        resp = stub.Register(
            chat_pb2.RegisterRequest(
                account=account, password=password, display_name=display_name
            ),
            timeout=TIMEOUT,
        )
        print(f'[register] {account} -> id={resp.user.id}')
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.ALREADY_EXISTS:
            resp = stub.Login(
                chat_pb2.LoginRequest(account=account, password=password),
                timeout=TIMEOUT,
            )
            print(f'[login]    {account} -> id={resp.user.id}')
        else:
            raise
    return resp.user


def ensure_conversation(alice_id, bob_id):
    if SKIP_DB_SEED:
        cid = int(FIXED_CONVERSATION_ID)
        print(f'[setup] using fixed conversation id={cid} (SKIP_DB_SEED, no local DB)')
        return cid
    conv = (
        Conversation.objects.filter(user1_id=alice_id, user2_id=bob_id).first()
        or Conversation.objects.filter(user1_id=bob_id, user2_id=alice_id).first()
    )
    if conv is None:
        conv = Conversation.objects.create(user1_id=alice_id, user2_id=bob_id)
        print(f'[setup] created conversation id={conv.id}')
    else:
        print(f'[setup] using existing conversation id={conv.id}')
    return conv.id


def test_get_users_status(stub):
    print('\n=== GetUsersStatus (unary) ===')
    resp = stub.GetUsersStatus(chat_pb2.GetUsersStatusRequest(), timeout=TIMEOUT)
    for u in resp.users:
        state = 'ONLINE' if u.is_online else 'offline'
        print(f'  {u.user_id} {u.username} ({u.display_name}) {state}')


def test_send_and_stream(stub, conv_id, alice_id, bob_id):
    print('\n=== SendMessage + StreamMessages (server streaming) ===')
    received = []

    def listen():
        try:
            for msg in stub.StreamMessages(
                chat_pb2.StreamMessagesRequest(conversation_id=conv_id),
                metadata=meta(bob_id),
            ):
                received.append(msg)
                print(f'  [bob via StreamMessages] {msg.sender_name}: {msg.content}')
        except grpc.RpcError as e:
            print(f'  [stream ended] {e.code()}')

    threading.Thread(target=listen, daemon=True).start()
    time.sleep(0.5)

    stub.SendMessage(
        chat_pb2.SendMessageRequest(
            conversation_id=conv_id, sender_id=0, content='hello from alice'
        ),
        metadata=meta(alice_id),
        timeout=TIMEOUT,
    )
    time.sleep(1.0)
    assert any(m.content == 'hello from alice' for m in received), 'bob did not receive alice message via StreamMessages'
    print('  PASS')


def test_sync_messages(stub, conv_id, alice_id):
    print('\n=== SyncMessages (client streaming) ===')

    def gen():
        for i in range(3):
            yield chat_pb2.SendMessageRequest(
                conversation_id=conv_id, sender_id=0, content=f'offline-{i}'
            )

    resp = stub.SyncMessages(gen(), metadata=meta(alice_id), timeout=TIMEOUT)
    print(f'  received={resp.received_count} failed={resp.failed_count} :: {resp.message}')
    assert resp.received_count == 3 and resp.failed_count == 0, 'SyncMessages did not accept the batch'
    print('  PASS')


def test_chat(stub, conv_id, alice_id, bob_id):
    print('\n=== Chat (bidirectional streaming) ===')
    received = []

    def bob_stream():
        for i in range(2):
            yield chat_pb2.ChatClientMessage(
                conversation_id=conv_id, sender_id=0, content=f'bob-bidi-{i}'
            )
            time.sleep(0.3)
        time.sleep(1.5)

    def alice_sends():
        time.sleep(0.6)
        for i in range(2):
            stub.SendMessage(
                chat_pb2.SendMessageRequest(
                    conversation_id=conv_id, sender_id=0, content=f'alice-{i}'
                ),
                metadata=meta(alice_id),
                timeout=TIMEOUT,
            )
            time.sleep(0.4)

    threading.Thread(target=alice_sends, daemon=True).start()

    stream = stub.Chat(bob_stream(), metadata=meta(bob_id))
    deadline = time.time() + 3
    for msg in stream:
        received.append(msg)
        print(f'  [bob via Chat] {msg.sender_name}: {msg.content}')
        if time.time() > deadline:
            break
    stream.cancel()

    assert any(m.content.startswith('alice-') for m in received), 'bob did not receive alice messages over Chat'
    assert not any(m.content.startswith('bob-bidi-') for m in received), 'bob received his own messages (self-echo bug)'
    print('  PASS')


def main():
    print(f'Target: {TARGET}')
    with grpc.insecure_channel(TARGET) as channel:
        stub = chat_pb2_grpc.ChatServiceStub(channel)

        alice = register_or_login(stub, 'alice_test', 'alice-pass', 'Alice')
        bob = register_or_login(stub, 'bob_test', 'bob-pass', 'Bob')
        conv_id = ensure_conversation(alice.id, bob.id)
        print(f'Using conversation id={conv_id}')

        test_get_users_status(stub)
        test_send_and_stream(stub, conv_id, alice.id, bob.id)
        test_sync_messages(stub, conv_id, alice.id)
        test_chat(stub, conv_id, alice.id, bob.id)

    print('\nALL TESTS PASSED')


if __name__ == '__main__':
    main()
