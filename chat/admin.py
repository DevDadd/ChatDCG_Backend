from django.contrib import admin

from chat.models import Conversation, Message, User


@admin.register(User)
class UserAdmin(admin.ModelAdmin):
    list_display = ("id", "username", "display_name", "is_online", "last_seen")
    search_fields = ("username", "display_name")


@admin.register(Conversation)
class ConversationAdmin(admin.ModelAdmin):
    list_display = ("id", "user1", "user2", "created_at")


@admin.register(Message)
class MessageAdmin(admin.ModelAdmin):
    list_display = ("id", "conversation", "sender", "created_at")
    list_filter = ("conversation", "sender")
    search_fields = ("content",)
