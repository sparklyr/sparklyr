#' @param max_bins The maximum number of bins used for discretizing
#'   continuous features and for choosing how to split on features at
#'   each node. More bins give higher granularity.
#' @param max_depth Maximum depth of the tree (>= 0); that is, the maximum
#'   number of nodes separating any leaves from the root of the tree.
#' @param min_info_gain Minimum information gain for a split to be considered
#'   at a tree node. Should be >= 0, defaults to 0.
#' @param min_instances_per_node Minimum number of instances each child must
#'   have after split.
#' @param seed Seed for random numbers.
#' @param checkpoint_interval Set checkpoint interval (>= 1) or disable checkpoint (-1).
#'   E.g. 10 means that the cache will get checkpointed every 10 iterations, defaults to 10.

#' @param cache_node_ids If \code{FALSE}, the algorithm will pass trees to executors to match instances with nodes.
#'   If \code{TRUE}, the algorithm will cache node IDs for each instance. Caching can speed up training of deeper trees.
#'   Defaults to \code{FALSE}.
#' @param max_memory_in_mb Maximum memory in MB allocated to histogram aggregation.
#'   If too small, then 1 node will be split per iteration,
#'   and its aggregates may exceed this size. Defaults to 256.
