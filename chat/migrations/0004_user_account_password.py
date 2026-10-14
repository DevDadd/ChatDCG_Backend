from django.db import migrations, models


class Migration(migrations.Migration):
    # The columns were added by hand in Supabase (see SQL in project notes);
    # apply this with `migrate chat --fake`.

    dependencies = [
        ("chat", "0003_conversation_message"),
    ]

    operations = [
        migrations.AddField(
            model_name="user",
            name="account",
            field=models.CharField(default="", max_length=100, unique=True),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name="user",
            name="password",
            field=models.CharField(default="", max_length=100),
            preserve_default=False,
        ),
    ]
