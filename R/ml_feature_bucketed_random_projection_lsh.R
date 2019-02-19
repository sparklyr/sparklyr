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
ft_bucketed_random_projection_lsh <- function(x, input_col = NULL, output_col = NULL,
                                              bucket_length = NULL, num_hash_tables = 1, seed = NULL,
                                              uid = random_string("bucketed_random_projection_lsh_"), ...) {
  check_dots_used()
  UseMethod("ft_bucketed_random_projection_lsh")
}

ml_bucketed_random_projection_lsh <- ft_bucketed_random_projection_lsh

#' @export
ft_bucketed_random_projection_lsh.spark_connection <- function(x, input_col = NULL, output_col = NULL,
                                                               bucket_length = NULL, num_hash_tables = 1, seed = NULL,
                                                               uid = random_string("bucketed_random_projection_lsh_"), ...) {
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
    x, "org.apache.spark.ml.feature.BucketedRandomProjectionLSH",
    input_col = .args[["input_col"]], output_col = .args[["output_col"]], uid = .args[["uid"]]
  ) %>%
    jobj_set_param("setBucketLength", .args[["bucket_length"]]) %>%
    invoke("setNumHashTables", .args[["num_hash_tables"]]) %>%
    jobj_set_param("setSeed", .args[["seed"]])

  estimator <- new_ml_bucketed_random_projection_lsh(jobj)

  estimator
}

#' @export
ft_bucketed_random_projection_lsh.ml_pipeline <- function(x, input_col = NULL, output_col = NULL,
                                                          bucket_length = NULL, num_hash_tables = 1, seed = NULL,
                                                          uid = random_string("bucketed_random_projection_lsh_"), ...) {
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
ft_bucketed_random_projection_lsh.tbl_spark <- function(x, input_col = NULL, output_col = NULL,
                                                        bucket_length = NULL, num_hash_tables = 1, seed = NULL,
                                                        uid = random_string("bucketed_random_projection_lsh_"), ...) {
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

  if (is_ml_transformer(stage))
    ml_transform(stage, x)
  else
    ml_fit_and_transform(stage, x)
}

new_ml_bucketed_random_projection_lsh <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_bucketed_random_projection_lsh")
}

new_ml_bucketed_random_projection_lsh_model <- function(jobj) {
  new_ml_transformer(
    jobj,
    approx_nearest_neighbors = make_approx_nearest_neighbors(jobj),
    approx_similarity_join = make_approx_similarity_join(jobj),
    class = "ml_bucketed_random_projection_lsh_model")
}

validator_ml_bucketed_random_projection_lsh <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["bucket_length"]] <- cast_nullable_scalar_double(.args[["bucket_length"]])
  .args[["num_hash_tables"]] <- cast_scalar_integer(.args[["num_hash_tables"]])
  .args[["seed"]] <- cast_nullable_scalar_integer(.args[["seed"]])
  .args
}
