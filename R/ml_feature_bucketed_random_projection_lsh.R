#' Feature Tranformation -- LSH (Estimator)
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
  x, input_col, output_col,
  bucket_length, num_hash_tables = 1L, seed = NULL,
  dataset = NULL,
  uid = random_string("bucketed_random_projection_lsh_"), ...) {
  UseMethod("ft_bucketed_random_projection_lsh")
}

#' @export
ft_bucketed_random_projection_lsh.spark_connection <- function(
  x, input_col, output_col,
  bucket_length, num_hash_tables = 1L, seed = NULL,
  dataset = NULL,
  uid = random_string("bucketed_random_projection_lsh_"), ...) {

  if (spark_version(x) < "2.1.0")
    stop("LSH is supported in Spark 2.1.0+")

  ml_ratify_args()

  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.BucketedRandomProjectionLSH",
                             input_col, output_col, uid) %>%
    invoke("setBucketLength", bucket_length) %>%
    invoke("setNumHashTables", num_hash_tables)

  if (!rlang::is_null(seed))
    jobj <- invoke(jobj, "setSeed", seed)

  estimator <- new_ml_bucketed_random_projection_lsh(jobj)

  if (is.null(dataset))
    estimator
  else
    ml_fit(estimator, dataset)
}

#' @export
ft_bucketed_random_projection_lsh.ml_pipeline <- function(
  x, input_col, output_col,
  bucket_length, num_hash_tables = 1L, seed = NULL,
  dataset = NULL,
  uid = random_string("bucketed_random_projection_lsh_"), ...
) {

  stage <- ml_new_stage_modified_args()
  ml_add_stage(x, stage)

}

#' @export
ft_bucketed_random_projection_lsh.tbl_spark <- function(
  x, input_col, output_col,
  bucket_length, num_hash_tables = 1L, seed = NULL,
  dataset = NULL,
  uid = random_string("bucketed_random_projection_lsh_"), ...
) {
  dots <- rlang::dots_list(...)

  stage <- ml_new_stage_modified_args()

  if (is_ml_transformer(stage))
    ml_transform(stage, x)
  else
    ml_fit_and_transform(stage, x)
}

ml_validator_bucketed_random_projection_lsh <- function(args, nms) {
  args %>%
    ml_validate_args({
      bucket_length <- ensure_scalar_double(bucket_length)
      num_hash_tables <- ensure_scalar_integer(num_hash_tables)
      if (!rlang::is_null(seed))
        seed <- ensure_scalar_integer(seed)
    }) %>%
    ml_extract_args(nms)
}

new_ml_bucketed_random_projection_lsh <- function(jobj) {
  new_ml_estimator(jobj, subclass = "ml_bucketed_random_projection_lsh")
}

new_ml_bucketed_random_projection_lsh_model <- function(jobj) {
  new_ml_transformer(
    jobj,
    approx_nearest_neighbors = make_approx_nearest_neighbors(jobj),
    approx_similarity_join = make_approx_similarity_join(jobj),
    subclass = "ml_bucketed_random_projection_lsh_model")
}
