from typing import Mapping

from csv_dataflow.gui.highlighting.modes import Highlighting
from csv_dataflow.relation import RelationPath


def hyperscript[S, T](
    highlighting: Mapping[RelationPath[S, T], set[Highlighting]],
) -> str:
    paths_by_highlight: dict[
        Highlighting, list[RelationPath[S, T]]
    ] = {}
    for path, highlights in highlighting.items():
        for highlight in highlights:
            if highlight not in paths_by_highlight:
                paths_by_highlight[highlight] = []

            paths_by_highlight[highlight].append(path)

    _ = ""
    for highlight, paths in paths_by_highlight.items():
        _ += (
            " on mouseover"
            " halt the event's bubbling"
            f" toggle .{highlight.lower()} on"
            f" [{",".join(path.as_id for path in paths)}]"
            " until mouseout"
        )

    return _
