#' @rdname ft_string_indexer
#' @param labels Vector of labels, corresponding to indices to be assigned.
#' @export
ft_string_indexer_model <- function(x, input_col = NULL, output_col = NULL, labels,
                                    handle_invalid = "error",
                                    uid = random_string("string_indexer_model_"), ...) {
  check_dots_used()
  UseMethod("ft_string_indexer_model")
}

ml_string_indexer_model <- ft_string_indexer_model

#' @export
ft_string_indexer_model.spark_connection <- function(x, input_col = NULL, output_col = NULL, labels,
                                                     handle_invalid = "error",
                                                     uid = random_string("string_indexer_model_"), ...) {
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
    x, "org.apache.spark.ml.feature.StringIndexerModel",
    .args[["uid"]], .args[["labels"]]
  ) %>%
    jobj_set_param("setInputCol", .args[["input_col"]]) %>%
    jobj_set_param("setOutputCol", .args[["output_col"]])

  new_ml_string_indexer_model(jobj)
}

#' @export
ft_string_indexer_model.ml_pipeline <- function(x, input_col = NULL, output_col = NULL, labels,
                                                handle_invalid = "error",
                                                uid = random_string("string_indexer_model_"), ...) {
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
ft_string_indexer_model.tbl_spark <- function(x, input_col = NULL, output_col = NULL, labels,
                                              handle_invalid = "error",
                                              uid = random_string("string_indexer_model_"), ...) {
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
  .args[["labels"]] <- cast_character_list(.args[["labels"]])
  .args[["handle_invalid"]] <- cast_choice(
    .args[["handle_invalid"]],
    c("error", "skip", "keep")
  )
  .args
}
