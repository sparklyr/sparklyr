#' @rdname ft_lsh
#' @export
ft_minhash_lsh <- function(
  x, input_col, output_col,
  num_hash_tables = 1L, seed = NULL,
  dataset = NULL,
  uid = random_string("minhash_lsh_"), ...) {
  UseMethod("ft_minhash_lsh")
}

#' @export
ft_minhash_lsh.spark_connection <- function(
  x, input_col, output_col,
  num_hash_tables = 1L, seed = NULL,
  dataset = NULL,
  uid = random_string("minhash_lsh_"), ...) {

  if (spark_version(x) < "2.1.0")
    stop("LSH is supported in Spark 2.1.0+")

  ml_ratify_args()

  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.MinHashLSH",
                             input_col, output_col, uid) %>%
    invoke("setNumHashTables", num_hash_tables)

  if (!rlang::is_null(seed))
    jobj <- invoke(jobj, "setSeed", seed)

  estimator <- new_ml_minhash_lsh(jobj)

  if (is.null(dataset))
    estimator
  else
    ml_fit(estimator, dataset)
}

#' @export
ft_minhash_lsh.ml_pipeline <- function(
  x, input_col, output_col,
  num_hash_tables = 1L, seed = NULL,
  dataset = NULL,
  uid = random_string("minhash_lsh_"), ...
) {

  stage <- ml_new_stage_modified_args()
  ml_add_stage(x, stage)

}

#' @export
ft_minhash_lsh.tbl_spark <- function(
  x, input_col, output_col,
  num_hash_tables = 1L, seed = NULL,
  dataset = NULL,
  uid = random_string("minhash_lsh_"), ...
) {
  dots <- rlang::dots_list(...)

  stage <- ml_new_stage_modified_args()

  if (is_ml_transformer(stage))
    ml_transform(stage, x)
  else
    ml_fit_and_transform(stage, x)
}

ml_validator_minhash_lsh <- function(args, nms) {
  args %>%
    ml_validate_args({
      num_hash_tables <- ensure_scalar_integer(num_hash_tables)
      if (!rlang::is_null(seed))
        seed <- ensure_scalar_integer(seed)
    }) %>%
    ml_extract_args(nms)
}

new_ml_minhash_lsh <- function(jobj) {
  new_ml_estimator(jobj, subclass = "ml_minhash_lsh")
}

new_ml_minhash_lsh_model <- function(jobj) {
  new_ml_transformer(
    jobj,
    approx_nearest_neighbors = make_approx_nearest_neighbors(jobj),
    approx_similarity_join = make_approx_similarity_join(jobj),
    subclass = "ml_minhash_lsh_model")
}
