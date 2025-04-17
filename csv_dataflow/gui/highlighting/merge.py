from itertools import chain
from typing import Iterable, Mapping

from csv_dataflow.gui.highlighting.modes import Highlighting
from csv_dataflow.relation import RelationPath


# This could have a more generic type
def merge_path_highlights[S, T](
    highlight_mappings: Iterable[
        Mapping[RelationPath[S, T], set[Highlighting]]
    ],
) -> Mapping[RelationPath[S, T], set[Highlighting]]:
    paths = set(
        chain.from_iterable(
            highlights.keys()
            for highlights in highlight_mappings
        )
    )
    return {
        path: set[Highlighting]().union(
            *(
                highlights[path]
                for highlights in highlight_mappings
                if path in highlights
            )
        )
        for path in paths
    }
