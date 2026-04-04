from __future__ import annotations


def format_fill_time(seconds: int) -> str:
    if seconds < 60:
        return f"~{seconds} seconds"
    minutes = seconds // 60
    return f"~{minutes} minute{'s' if minutes != 1 else ''}"
