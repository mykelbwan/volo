from core.tasks.presentation import (
    format_task_detail,
    format_task_line,
    task_latest_update_line,
    task_latest_update_text,
    user_facing_task_status,
)


def test_task_latest_update_helpers_trim_missing_values():
    assert task_latest_update_text({}) == ""
    assert task_latest_update_line({}) == ""
    assert task_latest_update_text({"latest_summary": "  Waiting for confirmation.  "}) == (
        "Waiting for confirmation."
    )
    assert task_latest_update_line({"latest_summary": "Waiting for confirmation."}) == (
        "Latest update: Waiting for confirmation."
    )


def test_format_task_detail_reuses_shared_status_and_update_copy():
    task = {
        "status": "WAITING_CONFIRMATION",
        "latest_summary": "Waiting for your confirmation.",
    }

    formatted = format_task_detail(task, task_label="Task 2: Swap STT to NIA")

    assert formatted == (
        "Task 2: Swap STT to NIA\n"
        "Status: Needs confirmation\n"
        "Latest update: Waiting for your confirmation."
    )


def test_format_task_line_falls_back_to_latest_update_when_title_missing():
    task = {
        "task_number": 3,
        "status": "WAITING_EXTERNAL",
        "latest_summary": "Bridge is still pending finality.",
    }

    assert format_task_line(task) == (
        "- Task 3: Bridge is still pending finality. (In progress)"
    )


def test_user_facing_task_status_handles_unknown_values():
    assert user_facing_task_status("SOMETHING_NEW") == "Updated"


def test_user_facing_task_status_maps_waiting_funds():
    assert user_facing_task_status("WAITING_FUNDS") == "Waiting for funds"
