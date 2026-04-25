from __future__ import annotations

import argparse
import os
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Listen for Rubika channel updates and print their object_guid values."
    )
    parser.add_argument(
        "--session",
        default=os.getenv("RUBIKA_SESSION", "rubika_session").strip() or "rubika_session",
        help="Rubpy session name/path to use. Defaults to RUBIKA_SESSION or rubika_session.",
    )
    return parser.parse_args()


def title_from_update(update: Any) -> str | None:
    for attr in ("title", "group_name", "channel_name"):
        value = getattr(update, attr, None)
        if value:
            return str(value)

    original = getattr(update, "original_update", None)
    if isinstance(original, dict):
        for key in ("title", "group_name", "channel_name"):
            value = original.get(key)
            if value:
                return str(value)

        nested_chat = original.get("chat") if isinstance(original.get("chat"), dict) else None
        if nested_chat:
            value = nested_chat.get("title")
            if value:
                return str(value)

    return None


def main() -> None:
    try:
        from rubpy import Client, filters
    except Exception as error:
        raise SystemExit(
            "rubpy is required to run this helper. Install project dependencies first. "
            f"Import error: {error}"
        )

    args = parse_args()
    app = Client(args.session)
    seen_guids: set[str] = set()

    @app.on_message_updates(filters.is_channel)
    async def on_update(update):
        object_guid = str(getattr(update, "object_guid", "") or "").strip()
        if not object_guid:
            return

        title = title_from_update(update) or "-"
        message_text = str(getattr(update, "text", "") or "").strip() or "-"

        prefix = "NEW" if object_guid not in seen_guids else "SEEN"
        seen_guids.add(object_guid)

        print("=" * 48, flush=True)
        print(f"{prefix} CHANNEL GUID: {object_guid}", flush=True)
        print(f"TITLE: {title}", flush=True)
        print(f"TEXT: {message_text}", flush=True)
        print("=" * 48, flush=True)

    print(
        "\n".join(
            [
                f"Listening for channel updates with session: {args.session}",
                "Post a message in the target Rubika channel.",
                "When an update arrives, copy the CHANNEL GUID value and use it in Walrus settings.",
            ]
        ),
        flush=True,
    )
    app.run()


if __name__ == "__main__":
    main()
