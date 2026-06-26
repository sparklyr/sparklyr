#' Feature Transformation -- StringIndexer (Estimator)
#'
#' A label indexer that maps a string column of labels to an ML column of
#'   label indices. If the input column is numeric, we cast it to string and
#'   index the string values. The indices are in \code{[0, numLabels)}, ordered by
#'   label frequencies. So the most frequent label gets index 0. This function
#'   is the inverse of \code{\link{ft_index_to_string}}.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-estimator-transformer
#' @template roxlate-ml-feature-handle-invalid
#' @param string_order_type (Spark 2.3+)How to order labels of string column.
#'  The first label after ordering is assigned an index of 0. Options are
#'  \code{"frequencyDesc"}, \code{"frequencyAsc"}, \code{"alphabetDesc"}, and \code{"alphabetAsc"}.
#'  Defaults to \code{"frequencyDesc"}.
#' @seealso \code{\link{ft_index_to_string}}
#' @export
ft_string_indexer <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  handle_invalid = "error",
  string_order_type = "frequencyDesc",
  uid = random_string("string_indexer_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_string_indexer")
}

ml_string_indexer <- ft_string_indexer

#' @export
ft_string_indexer.spark_connection <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  handle_invalid = "error",
  string_order_type = "frequencyDesc",
  uid = random_string("string_indexer_"),
  ...
) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    handle_invalid = handle_invalid,
    string_order_type = string_order_type,
    uid = uid
  ) %>%
    validator_ml_string_indexer()

  estimator <- spark_pipeline_stage(
    x,
    "org.apache.spark.ml.feature.StringIndexer",
    input_col = .args[["input_col"]],
    output_col = .args[["output_col"]],
    uid = .args[["uid"]]
  ) %>%
    jobj_set_param(
      "setHandleInvalid",
      .args[["handle_invalid"]],
      "2.1.0",
      "error"
    ) %>%
    jobj_set_param(
      "setStringOrderType",
      .args[["string_order_type"]],
      "2.3.0",
      "frequencyDesc"
    ) %>%
    new_ml_string_indexer()

  estimator
}

#' @export
ft_string_indexer.ml_pipeline <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  handle_invalid = "error",
  string_order_type = "frequencyDesc",
  uid = random_string("string_indexer_"),
  ...
) {
  stage <- ft_string_indexer.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    handle_invalid = handle_invalid,
    string_order_type = string_order_type,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_string_indexer.tbl_spark <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  handle_invalid = "error",
  string_order_type = "frequencyDesc",
  uid = random_string("string_indexer_"),
  ...
) {
  stage <- ft_string_indexer.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    handle_invalid = handle_invalid,
    string_order_type = string_order_type,
    uid = uid,
    ...
  )
  # backwards compatibility for params argument
  dots <- rlang::dots_list(...)
  if (rlang::has_name(dots, "params") && is.environment(dots$params)) {
    warning(
      "`params` has been deprecated and will be removed in a future release.",
      call. = FALSE
    )
    transformer <- if (is_ml_transformer(stage)) {
      stage
    } else {
      ml_fit(stage, x)
    }
    dots$params$labels <- spark_jobj(transformer) %>%
      invoke("labels") %>%
      as.character()
    transformer %>%
      ml_transform(x)
  } else {
    if (is_ml_transformer(stage)) {
      ml_transform(stage, x)
    } else {
      ml_fit_and_transform(stage, x)
    }
  }
}

new_ml_string_indexer <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_string_indexer")
}

new_ml_string_indexer_model <- function(jobj) {
  new_ml_transformer(
    jobj,
    labels = invoke(jobj, "labels") %>%
      as.character(),
    class = "ml_string_indexer_model"
  )
}

#' @rdname ft_string_indexer
#' @param model A fitted StringIndexer model returned by \code{ft_string_indexer()}
#' @return \code{ml_labels()} returns a vector of labels, corresponding to indices to be assigned.
#' @export
ml_labels <- function(model) model$labels

validator_ml_string_indexer <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["handle_invalid"]] <- cast_choice(
    .args[["handle_invalid"]],
    c("error", "skip", "keep")
  )
  .args[["string_order_type"]] <- cast_choice(
    .args[["string_order_type"]],
    c("frequencyDesc", "frequencyAsc", "alphabetDesc", "alphabetAsc")
  )
  .args
}

#' @rdname ft_string_indexer
#' @param labels Vector of labels, corresponding to indices to be assigned.
#' @export
ft_string_indexer_model <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  labels,
  handle_invalid = "error",
  uid = random_string("string_indexer_model_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_string_indexer_model")
}

ml_string_indexer_model <- ft_string_indexer_model

#' @export
ft_string_indexer_model.spark_connection <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  labels,
  handle_invalid = "error",
  uid = random_string("string_indexer_model_"),
  ...
) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    labels = labels,
    handle_invalid = handle_invalid,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_string_indexer_model()

  jobj <- invoke_new(
    x,
    "org.apache.spark.ml.feature.StringIndexerModel",
    .args[["uid"]],
    .args[["labels"]]
  ) %>%
    jobj_set_param("setInputCol", .args[["input_col"]]) %>%
    jobj_set_param("setOutputCol", .args[["output_col"]])

  new_ml_string_indexer_model(jobj)
}

#' @export
ft_string_indexer_model.ml_pipeline <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  labels,
  handle_invalid = "error",
  uid = random_string("string_indexer_model_"),
  ...
) {
  stage <- ft_string_indexer_model.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    labels = labels,
    handle_invalid = handle_invalid,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_string_indexer_model.tbl_spark <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  labels,
  handle_invalid = "error",
  uid = random_string("string_indexer_model_"),
  ...
) {
  stage <- ft_string_indexer_model.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    labels = labels,
    handle_invalid = handle_invalid,
    uid = uid,
    ...
  )
  ml_transform(stage, x)
}

validator_ml_string_indexer_model <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["labels"]] <- cast_string_list(.args[["labels"]])
  .args[["handle_invalid"]] <- cast_choice(
    .args[["handle_invalid"]],
    c("error", "skip", "keep")
  )
  .args
}

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
ft_index_to_string <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  labels = NULL,
  uid = random_string("index_to_string_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_index_to_string")
}

ml_index_to_string <- ft_index_to_string

#' @export
ft_index_to_string.spark_connection <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  labels = NULL,
  uid = random_string("index_to_string_"),
  ...
) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    labels = labels,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_index_to_string()

  jobj <- spark_pipeline_stage(
    x,
    "org.apache.spark.ml.feature.IndexToString",
    input_col = .args[["input_col"]],
    output_col = .args[["output_col"]],
    uid = .args[["uid"]]
  ) %>%
    jobj_set_param("setLabels", .args[["labels"]])

  new_ml_index_to_string(jobj)
}

#' @export
ft_index_to_string.ml_pipeline <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  labels = NULL,
  uid = random_string("index_to_string_"),
  ...
) {
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
ft_index_to_string.tbl_spark <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  labels = NULL,
  uid = random_string("index_to_string_"),
  ...
) {
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
  new_ml_transformer(jobj, class = "ml_index_to_string")
}

validator_ml_index_to_string <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["labels"]] <- cast_string_list(.args[["labels"]], allow_null = TRUE)
  .args
}
