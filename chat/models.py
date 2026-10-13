from django.db import models


class User(models.Model):
    username = models.CharField(max_length=100, unique=True)
    display_name = models.CharField(max_length=100)

    is_online = models.BooleanField(default=False)
    last_seen = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return self.username