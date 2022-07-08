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

ft_bucketed_random_projection_lsh_impl <- function(x, input_col = NULL, output_col = NULL,
                                                   bucket_length = NULL, num_hash_tables = 1, seed = NULL,
                                                   uid = random_string("bucketed_random_projection_lsh_"), ...) {
  spark_require_version(spark_connection(x), "2.1.0", "LSH")

  estimator_process(
    x = x,
    uid = uid,
    spark_class = "org.apache.spark.ml.feature.BucketedRandomProjectionLSH",
    r_class = "ml_bucketed_random_projection_lsh",
    invoke_steps = list(
      setInputCol = cast_nullable_string(input_col),
      setOutputCol = cast_nullable_string(output_col),
      setBucketLength = cast_nullable_scalar_double(bucket_length),
      setNumHashTables = cast_scalar_integer(num_hash_tables),
      setSeed = cast_nullable_scalar_integer(seed)
      )
  )
}

ml_bucketed_random_projection_lsh <- ft_bucketed_random_projection_lsh

#' @export
ft_bucketed_random_projection_lsh.spark_connection <- ft_bucketed_random_projection_lsh_impl

#' @export
ft_bucketed_random_projection_lsh.ml_pipeline <- ft_bucketed_random_projection_lsh_impl

#' @export
ft_bucketed_random_projection_lsh.tbl_spark <- ft_bucketed_random_projection_lsh_impl

new_ml_bucketed_random_projection_lsh_model <- function(jobj) {
  new_ml_transformer(
    jobj,
    approx_nearest_neighbors = make_approx_nearest_neighbors(jobj),
    approx_similarity_join = make_approx_similarity_join(jobj),
    class = "ml_bucketed_random_projection_lsh_model"
  )
}
