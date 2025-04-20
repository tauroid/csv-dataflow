from typing import Mapping

from csv_dataflow.gui.highlighting.modes import Highlighting
from csv_dataflow.relation import RelationPath


def hyperscript[S, T](
    highlighting: Mapping[RelationPath[S, T], set[Highlighting]],
) -> str:
    ids_by_highlight: dict[Highlighting, set[str]] = {}
    for path, highlights in highlighting.items():
        for highlight in highlights:
            if highlight not in ids_by_highlight:
                ids_by_highlight[highlight] = set()

            ids_by_highlight[highlight].add(path.as_id)

    _ = ""
    for highlight, ids in ids_by_highlight.items():
        _ += (
            " on mouseover"
            " halt the event's bubbling"
            f" toggle .{highlight.lower()} on"
            f" [{",".join('#' + id for id in ids)}]"
            " until mouseout"
        )

    return _
