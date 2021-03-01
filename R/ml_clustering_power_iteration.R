#' @include ml_clustering.R
#' @include ml_model_helpers.R
#' @include utils.R
NULL

#' Spark ML -- Power Iteration Clustering
#'
#' Power iteration clustering (PIC) is a scalable and efficient algorithm for clustering vertices of a graph given pairwise similarities as edge properties, described in the paper "Power Iteration Clustering" by Frank Lin and William W. Cohen. It computes a pseudo-eigenvector of the normalized affinity matrix of the graph via power iteration and uses it to cluster vertices. spark.mllib includes an implementation of PIC using GraphX as its backend. It takes an RDD of (srcId, dstId, similarity) tuples and outputs a model with the clustering assignments. The similarities must be nonnegative. PIC assumes that the similarity measure is symmetric. A pair (srcId, dstId) regardless of the ordering should appear at most once in the input data. If a pair is missing from input, their similarity is treated as zero.
#' @param x A ‘spark_connection’  or a ‘tbl_spark’.
#' @param k The number of clusters to create.
#' @param max_iter The maximum number of iterations to run.
#' @param init_mode This can be either “random”, which is the default, to use a random vector as vertex properties, or “degree” to use normalized sum similarities.
#' @param src_col Column in the input Spark dataframe containing 0-based indexes of all source vertices in the affinity matrix described in the PIC paper.
#' @param dst_col Column in the input Spark dataframe containing 0-based indexes of all destination vertices in the affinity matrix described in the PIC paper.
#' @param weight_col Column in the input Spark dataframe containing non-negative edge weights in the affinity matrix described in the PIC paper.
#' @param ... Optional arguments. Currently unused.
#' @return A 2-column R dataframe with columns named "id" and "cluster" describing the resulting cluster assignments
#'
#' @examples
#' \dontrun{
#'
#' library(sparklyr)
#'
#' sc <- spark_connect(master = "local")
#'
#' r1 <- 1
#' n1 <- 80L
#' r2 <- 4
#' n2 <- 80L
#'
#' gen_circle <- function(radius, num_pts) {
#'   # generate evenly distributed points on a circle centered at the origin
#'   seq(0, num_pts - 1) %>%
#'     lapply(
#'       function(pt) {
#'         theta <- 2 * pi * pt / num_pts
#'
#'         radius * c(cos(theta), sin(theta))
#'       }
#'     )
#' }
#'
#' guassian_similarity <- function(pt1, pt2) {
#'   dist2 <- sum((pt2 - pt1) ^ 2)
#'
#'   exp(-dist2 / 2)
#' }
#'
#' gen_pic_data <- function() {
#'   # generate points on 2 concentric circle centered at the origin and then
#'   # compute pairwise Gaussian similarity values of all unordered pair of
#'   # points
#'   n <- n1 + n2
#'   pts <- append(gen_circle(r1, n1), gen_circle(r2, n2))
#'   num_unordered_pairs <- n * (n - 1) / 2
#'
#'   src <- rep(0L, num_unordered_pairs)
#'   dst <- rep(0L, num_unordered_pairs)
#'   sim <- rep(0, num_unordered_pairs)
#'
#'   idx <- 1
#'   for (i in seq(2, n)) {
#'     for (j in seq(i - 1)) {
#'       src[[idx]] <- i - 1L
#'       dst[[idx]] <- j - 1L
#'       sim[[idx]] <- guassian_similarity(pts[[i]], pts[[j]])
#'       idx <- idx + 1
#'     }
#'   }
#'
#'   tibble::tibble(src = src, dst = dst, sim = sim)
#' }
#'
#' pic_data <- copy_to(sc, gen_pic_data())
#'
#' clusters <- ml_power_iteration(
#'   pic_data, src_col = "src", dst_col = "dst", weight_col = "sim", k = 2, max_iter = 40
#' )
#' print(clusters)
#'
#' }
#'
#' @export
ml_power_iteration <- function(x, k = 4, max_iter = 20, init_mode = "random",
                               src_col = "src", dst_col = "dst", weight_col = "weight",
                               ...) {
  check_dots_used()
  UseMethod("ml_power_iteration")
}

#' @export
ml_power_iteration.spark_connection <- function(x, k = 4, max_iter = 20,
                                                init_mode = "random",
                                                src_col = "src", dst_col = "dst", weight_col = "weight",
                                                ...) {
  if (spark_version(x) < "2.4") {
    stop("`ml_power_iteration()` is only supported in Spark 2.4 or above.")
  }

  .args <- list(
    k = k,
    max_iter = max_iter,
    init_mode = init_mode,
    src_col = src_col,
    dst_col = dst_col,
    weight_col = weight_col
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_power_iteration()

  jobj <- spark_pipeline_stage(
    x, "org.apache.spark.ml.clustering.PowerIterationClustering",
    uid = NULL,
    k = .args[["k"]], max_iter = .args[["max_iter"]]
  ) %>%
    invoke("setInitMode", .args[["init_mode"]]) %>%
    invoke("setSrcCol", .args[["src_col"]]) %>%
    invoke("setDstCol", .args[["dst_col"]]) %>%
    invoke("setWeightCol", .args[["weight_col"]])

  new_ml_power_iteration(jobj)
}

#' @export
ml_power_iteration.tbl_spark <- function(x, k = 4, max_iter = 20,
                                         init_mode = "random",
                                         src_col = "src", dst_col = "dst", weight_col = "weight",
                                         ...) {
  sc <- spark_connection(x)
  stage <- ml_power_iteration.spark_connection(
    x = sc,
    k = k,
    max_iter = max_iter,
    init_mode = init_mode,
    src_col = src_col,
    dst_col = dst_col,
    weight_col = weight_col,
    ...
  )
  clusters <- stage$.jobj %>%
    invoke("assignClusters", spark_dataframe(x)) %>%
    invoke(
      "select",
      list(
        invoke_new(sc, "org.apache.spark.sql.Column", "id"),
        invoke_new(sc, "org.apache.spark.sql.Column", "cluster")
      )
    ) %>%
    invoke("collect") %>%
    purrr::transpose() %>%
    lapply(unlist)
  names(clusters) <- c("id", "cluster")

  clusters_df <- tibble::as_tibble(clusters)
  clusters_df[order(clusters_df$id), ]
}

validator_ml_power_iteration <- function(.args) {
  .args[["k"]] <- cast_integer(.args[["k"]])
  .args[["max_iter"]] <- cast_scalar_integer(.args[["max_iter"]])
  .args[["src_col"]] <- cast_string(.args[["src_col"]])
  .args[["dst_col"]] <- cast_string(.args[["dst_col"]])
  .args[["weight_col"]] <- cast_string(.args[["weight_col"]])
  .args
}

new_ml_power_iteration <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_power_iteration")
}
