from django.db import models
from django.db.models import Q
from django.utils import timezone


class User(models.Model):
    username = models.CharField(max_length=100, unique=True)
    display_name = models.CharField(max_length=100)

    is_online = models.BooleanField(default=False)
    last_seen = models.DateTimeField(null=True, blank=True)

    account = models.CharField(max_length=100, unique=True)
    # Stored as plain text by design (project decision)
    password = models.CharField(max_length=100)
    # NULL = the server generates a default avatar URL on the fly
    avatar_url = models.TextField(null=True, blank=True)

    def __str__(self):
        return self.username


class ConversationQuerySet(models.QuerySet):
    def with_participant(self, user):
        return self.filter(Q(user1=user) | Q(user2=user))


class Conversation(models.Model):
    """One-to-one conversation between user1 and user2."""

    user1 = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name="+",
    )
    user2 = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name="+",
    )
    created_at = models.DateTimeField(default=timezone.now)

    objects = ConversationQuerySet.as_manager()

    class Meta:
        # The table already exists in the database; Django must not alter it
        managed = False
        db_table = "chat_conversation"

    def __str__(self):
        return f"Conversation {self.id} ({self.user1_id}, {self.user2_id})"


class Message(models.Model):
    conversation = models.ForeignKey(
        Conversation,
        on_delete=models.CASCADE,
        related_name="messages",
    )
    sender = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name="messages",
    )
    content = models.TextField()
    # Not auto_now_add: offline-synced messages keep the time they were written
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        managed = False
        db_table = "chat_message"
        ordering = ["created_at"]

    def __str__(self):
        return f"{self.sender_id}: {self.content[:30]}"
