from csv_dataflow.gui.highlighting.sop import Highlighting
from csv_dataflow.relation import Relation, RelationPath


def highlight_relation[S, T](
    relation: Relation[S, T, bool],
) -> set[tuple[Highlighting, RelationPath[S, T]]]:
    """
    Args:
      relation:
        Relation to highlight. The data is whether the relation
        node's descendants are the full set in the implicit
        "unfiltered" relation, or whether some are missing.
        If some are missing, we can't just light up that node's
        arrow, we have to progress to its children.
    """
