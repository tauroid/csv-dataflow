from itertools import chain
from typing import Iterable, Mapping

from csv_dataflow.gui.highlighting.modes import Highlighting
from csv_dataflow.relation import RelationPath


def merge_highlighting(
    highlighting: Iterable[Highlighting],
) -> Highlighting: ...


# FIXME make it a Collection[Highlighting], then we can deal
#       with weird shit in post
def merge_path_highlights[S, T](
    highlight_mappings: Iterable[
        Mapping[RelationPath[S, T], Highlighting]
    ],
) -> Mapping[RelationPath[S, T], Highlighting]:
    paths = set(
        chain.from_iterable(
            highlights.keys()
            for highlights in highlight_mappings
        )
    )
    return {
        path: merge_highlighting(
            highlights[path]
            for highlights in highlight_mappings
            if path in highlights
        )
        for path in paths
    }
