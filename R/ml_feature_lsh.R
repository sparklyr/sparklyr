#' Feature Transformation -- LSH (Estimator)
#'
#' Locality Sensitive Hashing functions for Euclidean distance
#'   (Bucketed Random Projection) and Jaccard distance (MinHash).
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-estimator-transformer
#' @param bucket_length The length of each hash bucket, a larger bucket lowers the
#'   false negative rate. The number of buckets will be (max L2 norm of input vectors) /
#'   bucketLength.
#' @param num_hash_tables Number of hash tables used in LSH OR-amplification. LSH
#'   OR-amplification can be used to reduce the false negative rate. Higher values
#'   for this param lead to a reduced false negative rate, at the expense of added
#'    computational complexity.
#' @template roxlate-ml-seed
#' @name ft_lsh
#' @seealso ft_lsh_utils
#' @export
ft_bucketed_random_projection_lsh <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  bucket_length = NULL,
  num_hash_tables = 1,
  seed = NULL,
  uid = random_string("bucketed_random_projection_lsh_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_bucketed_random_projection_lsh")
}

ml_bucketed_random_projection_lsh <- ft_bucketed_random_projection_lsh

#' @export
ft_bucketed_random_projection_lsh.spark_connection <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  bucket_length = NULL,
  num_hash_tables = 1,
  seed = NULL,
  uid = random_string("bucketed_random_projection_lsh_"),
  ...
) {
  spark_require_version(x, "2.1.0", "LSH")

  .args <- list(
    input_col = input_col,
    output_col = output_col,
    bucket_length = bucket_length,
    num_hash_tables = num_hash_tables,
    seed = seed,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_bucketed_random_projection_lsh()

  jobj <- spark_pipeline_stage(
    x,
    "org.apache.spark.ml.feature.BucketedRandomProjectionLSH",
    input_col = .args[["input_col"]],
    output_col = .args[["output_col"]],
    uid = .args[["uid"]]
  ) %>%
    jobj_set_param("setBucketLength", .args[["bucket_length"]]) %>%
    invoke("setNumHashTables", .args[["num_hash_tables"]]) %>%
    jobj_set_param("setSeed", .args[["seed"]])

  estimator <- new_ml_bucketed_random_projection_lsh(jobj)

  estimator
}

#' @export
ft_bucketed_random_projection_lsh.ml_pipeline <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  bucket_length = NULL,
  num_hash_tables = 1,
  seed = NULL,
  uid = random_string("bucketed_random_projection_lsh_"),
  ...
) {
  stage <- ft_bucketed_random_projection_lsh.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    bucket_length = bucket_length,
    num_hash_tables = num_hash_tables,
    seed = seed,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_bucketed_random_projection_lsh.tbl_spark <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  bucket_length = NULL,
  num_hash_tables = 1,
  seed = NULL,
  uid = random_string("bucketed_random_projection_lsh_"),
  ...
) {
  stage <- ft_bucketed_random_projection_lsh.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    bucket_length = bucket_length,
    num_hash_tables = num_hash_tables,
    seed = seed,
    uid = uid,
    ...
  )

  if (is_ml_transformer(stage)) {
    ml_transform(stage, x)
  } else {
    ml_fit_and_transform(stage, x)
  }
}

new_ml_bucketed_random_projection_lsh <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_bucketed_random_projection_lsh")
}

new_ml_bucketed_random_projection_lsh_model <- function(jobj) {
  new_ml_transformer(
    jobj,
    approx_nearest_neighbors = make_approx_nearest_neighbors(jobj),
    approx_similarity_join = make_approx_similarity_join(jobj),
    class = "ml_bucketed_random_projection_lsh_model"
  )
}

validator_ml_bucketed_random_projection_lsh <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["bucket_length"]] <- cast_nullable_scalar_double(.args[[
    "bucket_length"
  ]])
  .args[["num_hash_tables"]] <- cast_scalar_integer(.args[["num_hash_tables"]])
  .args[["seed"]] <- cast_nullable_scalar_integer(.args[["seed"]])
  .args
}

#' @rdname ft_lsh
#' @export
ft_minhash_lsh <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  num_hash_tables = 1L,
  seed = NULL,
  uid = random_string("minhash_lsh_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_minhash_lsh")
}

ml_minhash_lsh <- ft_minhash_lsh

#' @export
ft_minhash_lsh.spark_connection <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  num_hash_tables = 1L,
  seed = NULL,
  uid = random_string("minhash_lsh_"),
  ...
) {
  spark_require_version(x, "2.1.0", "MinHashLSH")

  .args <- list(
    input_col = input_col,
    output_col = output_col,
    num_hash_tables = num_hash_tables,
    seed = seed,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_minhash_lsh()

  jobj <- spark_pipeline_stage(
    x,
    "org.apache.spark.ml.feature.MinHashLSH",
    input_col = .args[["input_col"]],
    output_col = .args[["output_col"]],
    uid = .args[["uid"]]
  ) %>%
    invoke("setNumHashTables", .args[["num_hash_tables"]]) %>%
    jobj_set_param("setSeed", .args[["seed"]])

  estimator <- new_ml_minhash_lsh(jobj)

  estimator
}

#' @export
ft_minhash_lsh.ml_pipeline <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  num_hash_tables = 1L,
  seed = NULL,
  uid = random_string("minhash_lsh_"),
  ...
) {
  stage <- ft_minhash_lsh.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    num_hash_tables = num_hash_tables,
    seed = seed,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_minhash_lsh.tbl_spark <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  num_hash_tables = 1L,
  seed = NULL,
  uid = random_string("minhash_lsh_"),
  ...
) {
  stage <- ft_minhash_lsh.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    num_hash_tables = num_hash_tables,
    seed = seed,
    uid = uid,
    ...
  )

  if (is_ml_transformer(stage)) {
    ml_transform(stage, x)
  } else {
    ml_fit_and_transform(stage, x)
  }
}

new_ml_minhash_lsh <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_minhash_lsh")
}

new_ml_minhash_lsh_model <- function(jobj) {
  new_ml_transformer(
    jobj,
    approx_nearest_neighbors = make_approx_nearest_neighbors(jobj),
    approx_similarity_join = make_approx_similarity_join(jobj),
    class = "ml_minhash_lsh_model"
  )
}

validator_ml_minhash_lsh <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["num_hash_tables"]] <- cast_scalar_integer(.args[["num_hash_tables"]])
  .args[["seed"]] <- cast_nullable_scalar_integer(.args[["seed"]])
  .args
}

make_approx_nearest_neighbors <- function(jobj) {
  force(jobj)
  function(dataset, key, num_nearest_neighbors, dist_col = "distCol") {
    dataset <- spark_dataframe(dataset)
    key <- spark_dense_vector(spark_connection(jobj), key)
    num_nearest_neighbors <- cast_scalar_integer(num_nearest_neighbors)
    dist_col <- cast_string(dist_col)
    jobj %>%
      invoke(
        "approxNearestNeighbors",
        dataset,
        key,
        num_nearest_neighbors,
        dist_col
      ) %>%
      sdf_register()
  }
}

make_approx_similarity_join <- function(jobj) {
  function(dataset_a, dataset_b, threshold, dist_col = "distCol") {
    sc <- spark_connection(jobj)
    dataset_a <- spark_dataframe(dataset_a)
    dataset_b <- spark_dataframe(dataset_b)
    threshold <- cast_scalar_double(threshold)
    dist_col <- cast_string(dist_col)
    jobj %>%
      invoke(
        "approxSimilarityJoin",
        dataset_a,
        dataset_b,
        threshold,
        dist_col
      ) %>%
      invoke(
        "select",
        list(
          spark_sql_column(sc, "datasetA.id", "id_a"),
          spark_sql_column(sc, "datasetB.id", "id_b"),
          spark_sql_column(sc, dist_col)
        )
      ) %>%
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
  model,
  dataset,
  key,
  num_nearest_neighbors,
  dist_col = "distCol"
) {
  model$approx_nearest_neighbors(dataset, key, num_nearest_neighbors, dist_col)
}

#' @rdname ft_lsh_utils
#' @param dataset_a One of the datasets to join.
#' @param dataset_b Another dataset to join.
#' @param threshold The threshold for the distance of row pairs.
#' @export
ml_approx_similarity_join <- function(
  model,
  dataset_a,
  dataset_b,
  threshold,
  dist_col = "distCol"
) {
  model$approx_similarity_join(dataset_a, dataset_b, threshold, dist_col)
}
