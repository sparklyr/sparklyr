make_approx_nearest_neighbors <- function(jobj) {
  force(jobj)
  function(dataset, key, num_nearest_neighbors, dist_col = "distCol") {
    dataset <- spark_dataframe(dataset)
    key <- spark_dense_vector(spark_connection(jobj), key)
    num_nearest_neighbors <- ensure_scalar_integer(num_nearest_neighbors)
    dist_col <- ensure_scalar_character(dist_col)
    jobj %>%
      invoke("approxNearestNeighbors",
             dataset, key, num_nearest_neighbors, dist_col) %>%
      sdf_register()
  }
}

make_approx_similarity_join <- function(jobj) {
  function(dataset_a, dataset_b, threshold, dist_col = "distCol") {
    sc <- spark_connection(jobj)
    dataset_a <- spark_dataframe(dataset_a)
    dataset_b <- spark_dataframe(dataset_b)
    threshold <- ensure_scalar_double(threshold)
    dist_col <- ensure_scalar_character(dist_col)
    jobj %>%
      invoke("approxSimilarityJoin",
             dataset_a, dataset_b, threshold, dist_col) %>%
      invoke("select", list(
        spark_sql_column(sc, "datasetA.id", "id_a"),
        spark_sql_column(sc, "datasetB.id", "id_b"),
        spark_sql_column(sc, dist_col)
      )) %>%
      sdf_register()
  }
}

#' Utility functions for LSH models
#'
#' @name ft_lsh_utils
#' @param model A fitted LSH model, returned by either \code{ft_minhash_lsh()}
#'   or \code{ft_bucketed_random_projection_lsh()}.
#' @param dataset The dataset to search for nearest neighbors of the key.
#' @param key Feature vector representing the item to search for.
#' @param num_nearest_neighbors The maximum number of nearest neighbors.
#' @param dist_col Output column for storing the distance between each result row and the key.
#' @export
ml_approx_nearest_neighbors <- function(
  model, dataset, key, num_nearest_neighbors, dist_col = "distCol"
) {
  model$approx_nearest_neighbors(dataset, key, num_nearest_neighbors, dist_col)
}

#' @rdname ft_lsh_utils
#' @param dataset_a One of the datasets to join.
#' @param dataset_b Another dataset to join.
#' @param threshold The threshold for the distance of row pairs.
#' @export
ml_approx_similarity_join <- function(
  model, dataset_a, dataset_b, threshold, dist_col = "distCol"
) {
  model$approx_similarity_join(dataset_a, dataset_b, threshold, dist_col)
}
