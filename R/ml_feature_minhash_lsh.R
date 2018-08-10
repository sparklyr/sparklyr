#' @rdname ft_lsh
#' @export
ft_minhash_lsh <- function(x, input_col = NULL, output_col = NULL,
                           num_hash_tables = 1L, seed = NULL, dataset = NULL,
                           uid = random_string("minhash_lsh_"), ...) {
  UseMethod("ft_minhash_lsh")
}

#' @export
ft_minhash_lsh.spark_connection <- function(x, input_col = NULL, output_col = NULL,
                                            num_hash_tables = 1L, seed = NULL, dataset = NULL,
                                            uid = random_string("minhash_lsh_"), ...) {

  spark_require_version(x, "2.1.0", "MinHashLSH")

  .args <- list(
    input_col = input_col,
    output_col = output_col,
    num_hash_tables = num_hash_tables,
    seed = seed,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    ml_validator_minhash_lsh()

  jobj <- ml_new_transformer(
    x, "org.apache.spark.ml.feature.MinHashLSH",
    input_col = .args[["input_col"]], output_col = .args[["output_col"]], uid = .args[["uid"]]
  ) %>%
    invoke("setNumHashTables", .args[["num_hash_tables"]]) %>%
    maybe_set_param("setSeed", .args[["seed"]])

  estimator <- new_ml_minhash_lsh(jobj)

  if (is.null(dataset))
    estimator
  else
    ml_fit(estimator, dataset)
}

#' @export
ft_minhash_lsh.ml_pipeline <- function(x, input_col = NULL, output_col = NULL,
                                       num_hash_tables = 1L, seed = NULL, dataset = NULL,
                                       uid = random_string("minhash_lsh_"), ...) {

  stage <- ft_minhash_lsh.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    num_hash_tables = num_hash_tables,
    seed = seed,
    dataset = dataset,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)

}

#' @export
ft_minhash_lsh.tbl_spark <- function(x, input_col = NULL, output_col = NULL,
                                     num_hash_tables = 1L, seed = NULL, dataset = NULL,
                                     uid = random_string("minhash_lsh_"), ...) {
  stage <- ft_minhash_lsh.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    num_hash_tables = num_hash_tables,
    seed = seed,
    dataset = dataset,
    uid = uid,
    ...
  )

  if (is_ml_transformer(stage))
    ml_transform(stage, x)
  else
    ml_fit_and_transform(stage, x)
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

ml_validator_minhash_lsh <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["num_hash_tables"]] <- cast_scalar_integer(.args[["num_hash_tables"]])
  .args[["seed"]] <- cast_nullable_scalar_integer(.args[["seed"]])
  .args
}
