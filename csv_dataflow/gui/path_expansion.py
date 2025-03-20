from typing import Any, Literal, MutableMapping

from csv_dataflow.gui.state.pickler import attach_pickle_store
from csv_dataflow.gui.state.triple import TripleState
from csv_dataflow.relation import Relation, RelationPath
from csv_dataflow.sop import (
    SumProductNode,
    SumProductPath,
)


def set_session_point_path_expanded(
    session: MutableMapping[str, bytes],
    name: str,
    point: Literal["Source", "Target"],
    path: SumProductPath[Any],
    yes: bool = True,
) -> tuple[SumProductNode[Any, bool], Relation[Any, Any]]:
    """Returns new visible sop for the point"""
    state = TripleState[Any, Any].uninitialised()
    attach_pickle_store(state, session, name)

    match point:
        case "Source":
            sop = state.user_state.source
        case "Target":
            sop = state.user_state.target

    # Expand / collapse the path
    expanded = sop.expanded
    expanded = expanded.replace_data_at(path, yes)
    if yes:
        # Expand recursion NOTE no longer understand
        for child in expanded.at(path).children:
            child_path = (*path, child)
            expanded = expanded.replace_at(
                child_path,
                expanded.at(child_path).map_data(
                    lambda _: False
                ),
            )

    # Save and recalculate visible stuff
    sop.expanded = expanded
    state.recalculate_visible()

    match point:
        case "Source":
            sop = state.visible.source
        case "Target":
            sop = state.visible.target

    return sop, state.visible.relation


def set_session_path_expanded(
    session: MutableMapping[str, bytes],
    name: str,
    path: RelationPath[Any, Any],
    yes: bool = True,
) -> tuple[SumProductNode[Any, bool], Relation[Any, Any]]:
    return set_session_point_path_expanded(
        session, name, path.point, path.sop_path, yes
    )
