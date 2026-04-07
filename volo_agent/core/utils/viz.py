from core.planning.execution_plan import ExecutionPlan, ExecutionState


def get_status_table(plan: ExecutionPlan, state: ExecutionState) -> str:
    if not plan or not state:
        return ""

    header = f"{'ID':<15} | {'TOOL':<10} | {'STATUS':<10} | {'PROGRESS':<15}"
    separator = "-" * len(header)
    rows = [header, separator]

    for node_id, node in plan.nodes.items():
        node_state = state.node_states.get(node_id)
        status = node_state.status.upper() if node_state else "PENDING"

        # Determine progress bar based on status
        if status == "SUCCESS":
            progress = "[##########] 100%"
        elif status == "RUNNING":
            progress = "[#####-----] 50%"
        elif status == "FAILED":
            progress = "[XX--------] FAIL"
        else:
            progress = "[----------] 0%"

        rows.append(f"{node_id:<15} | {node.tool:<10} | {status:<10} | {progress:<15}")

    return "\n".join(rows)
