#' Feature Transformation -- IndexToString (Transformer)
#'
#' A Transformer that maps a column of indices back to a new column of
#'   corresponding string values. The index-string mapping is either from
#'   the ML attributes of the input column, or from user-supplied labels
#'    (which take precedence over ML attributes). This function is the inverse
#'    of \code{\link{ft_string_indexer}}.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#'
#' @param labels Optional param for array of labels specifying index-string mapping.
#' @seealso \code{\link{ft_string_indexer}}
#' @export
ft_index_to_string <- function(x, input_col = NULL, output_col = NULL, labels = NULL,
                               uid = random_string("index_to_string_"), ...) {
  UseMethod("ft_index_to_string")
}

#' @export
ft_index_to_string.spark_connection <- function(x, input_col = NULL, output_col = NULL, labels = NULL,
                                                uid = random_string("index_to_string_"), ...) {

  .args <- list(
    input_col = input_col,
    output_col = output_col,
    labels = labels,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    ml_validator_index_to_string()

  jobj <- ml_new_transformer(
    x, "org.apache.spark.ml.feature.IndexToString",
    input_col = .args[["input_col"]], output_col = .args[["output_col"]], uid = .args[["uid"]]
  ) %>%
    maybe_set_param("setLabels", .args[["labels"]])

  new_ml_index_to_string(jobj)
}

#' @export
ft_index_to_string.ml_pipeline <- function(x, input_col = NULL, output_col = NULL, labels = NULL,
                                           uid = random_string("index_to_string_"), ...) {

  stage <- ft_index_to_string.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    labels = labels,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_index_to_string.tbl_spark <- function(x, input_col = NULL, output_col = NULL, labels = NULL,
                                         uid = random_string("index_to_string_"), ...) {

  stage <- ft_index_to_string.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    labels = labels,
    uid = uid,
    ...
  )
  ml_transform(stage, x)
}

new_ml_index_to_string <- function(jobj) {
  new_ml_transformer(jobj, subclass = "ml_index_to_string")
}

ml_validator_index_to_string <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["labels"]] <- cast_nullable_string_list(.args[["labels"]])
  .args
}
