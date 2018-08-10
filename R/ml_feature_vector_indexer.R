#' Feature Transformation -- VectorIndexer (Estimator)
#'
#' Indexing categorical feature columns in a dataset of Vector.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-estimator-transformer
#' @param max_categories Threshold for the number of values a categorical feature can take. If a feature is found to have > \code{max_categories} values, then it is declared continuous. Must be greater than or equal to 2. Defaults to 20.
#'
#' @export
ft_vector_indexer <- function(x, input_col = NULL, output_col = NULL,
                              max_categories = 20, dataset = NULL,
                              uid = random_string("vector_indexer_"), ...) {
  UseMethod("ft_vector_indexer")
}

#' @export
ft_vector_indexer.spark_connection <- function(x, input_col = NULL, output_col = NULL,
                                               max_categories = 20, dataset = NULL,
                                               uid = random_string("vector_indexer_"), ...) {

  .args <- list(
    input_col = input_col,
    output_col = output_col,
    max_categories = max_categories,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    ml_validator_vector_indexer()

  estimator <- ml_new_transformer(
    x, "org.apache.spark.ml.feature.VectorIndexer",
    input_col = .args[["input_col"]], output_col = .args[["output_col"]], uid = .args[["uid"]]) %>%
    invoke("setMaxCategories", .args[["max_categories"]]) %>%
    new_ml_vector_indexer()

  if (is.null(dataset))
    estimator
  else
    ml_fit(estimator, dataset)
}

#' @export
ft_vector_indexer.ml_pipeline <- function(x, input_col = NULL, output_col = NULL,
                                          max_categories = 20, dataset = NULL,
                                          uid = random_string("vector_indexer_"), ...) {

  stage <- ft_vector_indexer.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    max_categories = max_categories,
    dataset = dataset,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)

}

#' @export
ft_vector_indexer.tbl_spark <- function(x, input_col = NULL, output_col = NULL,
                                        max_categories = 20, dataset = NULL,
                                        uid = random_string("vector_indexer_"), ...) {
  stage <- ft_vector_indexer.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    max_categories = max_categories,
    dataset = dataset,
    uid = uid,
    ...
  )

  if (is_ml_transformer(stage))
    ml_transform(stage, x)
  else
    ml_fit_and_transform(stage, x)
}

new_ml_vector_indexer <- function(jobj) {
  new_ml_estimator(jobj, subclass = "ml_vector_indexer")
}

new_ml_vector_indexer_model <- function(jobj) {
  new_ml_transformer(jobj, subclass = "ml_vector_indexer_model")
}

ml_validator_vector_indexer <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["max_categories"]] <- cast_scalar_integer(.args[["max_categories"]])
  .args
}
