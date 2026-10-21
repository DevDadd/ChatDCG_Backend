# End-to-end test for the deployed ChatService.
# Covers: unary (GetUsersStatus, GetConversationHistory), client stream (SyncMessages),
# bidirectional (Chat), plus Register/Login.
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
    assert resp.user.avatar_url, 'Register/Login must return avatar_url'
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


def test_get_users_status(stub, caller_id):
    print('\n=== GetUsersStatus (unary) ===')
    resp = stub.GetUsersStatus(
        chat_pb2.GetUsersStatusRequest(),
        metadata=meta(caller_id),
        timeout=TIMEOUT,
    )
    ids = [u.user_id for u in resp.users]
    for u in resp.users:
        state = 'ONLINE' if u.is_online else 'offline'
        print(f'  {u.user_id} {u.username} ({u.display_name}) {state} avatar={u.avatar_url}')
    assert caller_id not in ids, 'caller should be excluded from GetUsersStatus'
    assert all(u.avatar_url for u in resp.users), 'every user must have avatar_url'
    print('  PASS')


def test_get_conversation_history(stub, caller_id, other_user_id):
    print('\n=== GetConversationHistory (unary) ===')
    resp = stub.GetConversationHistory(
        chat_pb2.GetConversationHistoryRequest(
            other_user_id=other_user_id,
        ),
        metadata=meta(caller_id),
        timeout=TIMEOUT,
    )
    print(f'  total_count={resp.total_count} returned={len(resp.messages)}')
    for m in resp.messages:
        print(f'  [{m.message_id}] {m.sender_name}: {m.content} ({m.timestamp})')
    assert resp.total_count == len(resp.messages), 'total_count must equal number of messages returned'
    print('  PASS')


def test_get_conversation_history_by_id(stub, conv_id, caller_id):
    print('\n=== GetConversationHistory by conversation_id (unary) ===')
    resp = stub.GetConversationHistory(
        chat_pb2.GetConversationHistoryRequest(
            conversation_id=conv_id,
        ),
        metadata=meta(caller_id),
        timeout=TIMEOUT,
    )
    print(f'  conversation_id={resp.conversation_id} total_count={resp.total_count} returned={len(resp.messages)}')
    assert resp.conversation_id == conv_id, 'response conversation_id must match'
    assert resp.total_count == len(resp.messages), 'total_count must equal number of messages returned'
    print('  PASS')


def test_get_conversation_history_auto_create(stub, caller_id, other_user_id):
    print('\n=== GetConversationHistory auto-create (unary) ===')
    resp = stub.GetConversationHistory(
        chat_pb2.GetConversationHistoryRequest(
            conversation_id=0,
            other_user_id=other_user_id,
        ),
        metadata=meta(caller_id),
        timeout=TIMEOUT,
    )
    print(f'  conversation_id={resp.conversation_id} total_count={resp.total_count} returned={len(resp.messages)}')
    assert resp.conversation_id != 0, 'must return newly created conversation_id'
    assert resp.total_count == 0, 'new conversation must have no messages'
    assert resp.total_count == len(resp.messages), 'total_count must equal number of messages returned'
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
    alice_received = []
    bob_received = []

    def chat_collect(user_id, out_msgs, into):
        stream = stub.Chat(out_msgs, metadata=meta(user_id))
        for msg in stream:
            into.append(msg)
            print(f'  [{user_id} via Chat] {msg.sender_name}: {msg.content}')

    def alice_out():
        time.sleep(0.6)
        for i in range(2):
            yield chat_pb2.ChatClientMessage(
                conversation_id=conv_id, sender_id=0, content=f'alice-{i}'
            )
            time.sleep(0.4)

    def bob_out():
        for i in range(2):
            yield chat_pb2.ChatClientMessage(
                conversation_id=conv_id, sender_id=0, content=f'bob-bidi-{i}'
            )
            time.sleep(0.3)
        time.sleep(1.5)

    t_alice = threading.Thread(
        target=lambda: chat_collect(alice_id, alice_out(), alice_received), daemon=True
    )
    t_bob = threading.Thread(
        target=lambda: chat_collect(bob_id, bob_out(), bob_received), daemon=True
    )
    t_alice.start()
    t_bob.start()
    t_alice.join(timeout=10)
    t_bob.join(timeout=10)

    alice_contents = [m.content for m in alice_received]
    bob_contents = [m.content for m in bob_received]
    assert any(c.startswith('bob-bidi-') for c in alice_contents), 'alice did not receive bob messages over Chat'
    assert any(c.startswith('alice-') for c in bob_contents), 'bob did not receive alice messages over Chat'
    assert not any(c.startswith('alice-') for c in alice_contents), 'alice received her own messages (self-echo bug)'
    assert not any(c.startswith('bob-bidi-') for c in bob_contents), 'bob received his own messages (self-echo bug)'
    print('  PASS')


def main():
    print(f'Target: {TARGET}')
    with grpc.insecure_channel(TARGET) as channel:
        stub = chat_pb2_grpc.ChatServiceStub(channel)

        alice = register_or_login(stub, 'alice_test', 'alice-pass', 'Alice')
        bob = register_or_login(stub, 'bob_test', 'bob-pass', 'Bob')
        charlie = register_or_login(stub, 'charlie_test', 'charlie-pass', 'Charlie')
        conv_id = ensure_conversation(alice.id, bob.id)
        print(f'Using conversation id={conv_id}')

        test_get_users_status(stub, alice.id)
        test_get_conversation_history(stub, alice.id, bob.id)
        test_get_conversation_history_by_id(stub, conv_id, alice.id)
        test_get_conversation_history_auto_create(stub, alice.id, charlie.id)
        test_sync_messages(stub, conv_id, alice.id)
        test_chat(stub, conv_id, alice.id, bob.id)

    print('\nALL TESTS PASSED')


if __name__ == '__main__':
    main()

