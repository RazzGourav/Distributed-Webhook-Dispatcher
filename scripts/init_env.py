"""Create local settings without printing the generated API key."""

import os
import secrets
from pathlib import Path

root = Path(__file__).resolve().parents[1]
content = (
    (root / ".env.example")
    .read_text()
    .replace("API_KEY=\n", f"API_KEY={secrets.token_urlsafe(32)}\n")
)
try:
    fd = os.open(root / ".env", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
except FileExistsError:
    raise SystemExit(".env already exists; left unchanged") from None
with os.fdopen(fd, "w") as file:
    file.write(content)
print("Created .env. Read API_KEY locally when authorizing Swagger or curl.")
