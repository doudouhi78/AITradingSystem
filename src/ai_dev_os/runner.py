from __future__ import annotations

from ai_dev_os.agents import approval_node
from ai_dev_os.agents import recorder_agent
from ai_dev_os.graph import graph
from ai_dev_os.io_utils import load_state_snapshot
from ai_dev_os.io_utils import save_state_snapshot
from ai_dev_os.state import KernelState
from ai_dev_os.validation_contract import assert_formal_roles_use_live
from ai_dev_os.validation_contract import assert_system_pipeline


def run_new_task(project_id: str, goal: str, human_decision: str = "", *, execution_source: str = "system_pipeline") -> KernelState:
    assert_system_pipeline(execution_source)
    assert_formal_roles_use_live()
    payload = {
        "project_id": project_id,
        "goal": goal,
    }
    if human_decision:
        payload["human_decision"] = human_decision
    result = graph.invoke(payload)
    save_state_snapshot(project_id, result)
    return result


def resume_pending_approval(project_id: str, decision: str, *, execution_source: str = "system_pipeline") -> KernelState:
    assert_system_pipeline(execution_source)
    assert_formal_roles_use_live()
    state = load_state_snapshot(project_id)
    if state.get("approval_status") != "pending":
        raise ValueError(f"Project {project_id} is not waiting for approval.")

    resumed_state = {
        **state,
        "human_decision": decision,
    }
    after_approval = approval_node(resumed_state)

    if after_approval["approval_status"] == "approved":
        final_state = recorder_agent(after_approval)
    else:
        final_state = after_approval

    save_state_snapshot(project_id, final_state)
    return final_state
