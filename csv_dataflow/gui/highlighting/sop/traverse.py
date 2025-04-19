from typing import Callable, Iterable


def traverse[N, K, Ct, Cd, I, R](
    yield_children: Callable[[N], Iterable[tuple[K, N]]],
    refine_context: Callable[[Ct, K, N], Ct],
    combine_child_data: Callable[
        [Ct, N, Iterable[tuple[K, Cd]]], Cd
    ],
    evaluate: Callable[[Ct, Cd], I],
    result: Callable[[I, Iterable[tuple[K, R]]], R],
    context: Ct,
    node: N,
) -> tuple[Cd, R]:
    child_results = tuple(
        (
            key,
            traverse(
                yield_children,
                refine_context,
                combine_child_data,
                evaluate,
                result,
                refine_context(context, key, child),
                child,
            ),
        )
        for key, child in yield_children(node)
    )
    subtree_summary = combine_child_data(
        context,
        node,
        (
            (key, child_subtree_summary)
            for key, (child_subtree_summary, _) in child_results
        ),
    )
    node_result = evaluate(context, subtree_summary)
    return subtree_summary, result(
        node_result,
        (
            (key, child_final_result)
            for key, (_, child_final_result) in child_results
        ),
    )
