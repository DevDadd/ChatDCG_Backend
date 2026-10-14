import json

from django.db import IntegrityError
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from chat.models import User

MAX_FIELD_LENGTH = 100


def user_to_dict(user):
    return {
        "id": user.id,
        "account": user.account,
        "username": user.username,
        "display_name": user.display_name,
    }


def error(message, status):
    return JsonResponse({"error": message}, status=status)


def read_json(request):
    try:
        data = json.loads(request.body or b"{}")
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    return data if isinstance(data, dict) else None


def clean_field(data, name):
    value = data.get(name)
    return value.strip() if isinstance(value, str) else ""


@csrf_exempt
@require_POST
def register(request):
    data = read_json(request)
    if data is None:
        return error("Body must be a JSON object", 400)

    account = clean_field(data, "account")
    # Not stripped: spaces in a password are deliberate
    password = data.get("password") if isinstance(data.get("password"), str) else ""
    display_name = clean_field(data, "display_name") or account
    username = clean_field(data, "username") or account

    if not account or not password:
        return error("account and password are required", 400)

    if any(
        len(value) > MAX_FIELD_LENGTH
        for value in (account, password, display_name, username)
    ):
        return error(f"Fields must be at most {MAX_FIELD_LENGTH} characters", 400)

    if User.objects.filter(account=account).exists():
        return error("Account already exists", 409)

    if User.objects.filter(username=username).exists():
        return error("Username already exists", 409)

    try:
        user = User.objects.create(
            account=account,
            password=password,
            username=username,
            display_name=display_name,
        )
    except IntegrityError:
        # Lost a race with a concurrent registration
        return error("Account or username already exists", 409)

    return JsonResponse({"user": user_to_dict(user)}, status=201)


@csrf_exempt
@require_POST
def login(request):
    data = read_json(request)
    if data is None:
        return error("Body must be a JSON object", 400)

    account = clean_field(data, "account")
    password = data.get("password") if isinstance(data.get("password"), str) else ""

    user = User.objects.filter(account=account, password=password).first()
    if not account or not password or user is None:
        return error("Invalid account or password", 401)

    return JsonResponse({"user": user_to_dict(user)})
