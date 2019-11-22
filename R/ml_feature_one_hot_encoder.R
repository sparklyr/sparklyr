#' Feature Transformation -- OneHotEncoder (Transformer)
#'
#' One-hot encoding maps a column of label indices to a column of binary
#' vectors, with at most a single one-value. This encoding allows algorithms
#' which expect continuous features, such as Logistic Regression, to use
#' categorical features. Typically, used with  \code{ft_string_indexer()} to
#' index a column first.
#'
#' @param input_cols The name of the input columns.
#' @param output_cols The name of the output columns.
#' @template roxlate-ml-feature-handle-invalid
#' @template roxlate-ml-feature-transformer
#' @param drop_last Whether to drop the last category. Defaults to \code{TRUE}.
#'
#' @export
ft_one_hot_encoder <- function(x, input_cols = NULL, output_cols = NULL, handle_invalid = NULL,
                               drop_last = TRUE, uid = random_string("one_hot_encoder_"), ...) {
  check_dots_used()
  UseMethod("ft_one_hot_encoder")
}

ml_one_hot_encoder <- ft_one_hot_encoder

#' @export
ft_one_hot_encoder.spark_connection <- function(x, input_cols = NULL, output_cols = NULL, handle_invalid = "error",
                                                drop_last = TRUE, uid = random_string("one_hot_encoder_"), ...) {
  .args <- list(
    input_cols = input_cols,
    output_cols = output_cols,
    handle_invalid = handle_invalid,
    drop_last = drop_last,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_one_hot_encoder()
  if(is_required_spark(x, "3.0.0")) {
    estimator <- spark_pipeline_stage(
      x, "org.apache.spark.ml.feature.OneHotEncoder",
      input_cols = .args[["input_cols"]], output_cols = .args[["output_cols"]], uid = .args[["uid"]]
    ) %>%
      invoke("setHandleInvalid", .args[["handle_invalid"]]) %>%
      invoke("setDropLast", .args[["drop_last"]]) %>%
      new_ml_one_hot_encoder()
  } else {
    if (length(.args[["input_cols"]]) > 1 || length(.args[["output_cols"]]) > 1) {
      stop("OneHotEncoder does not support encoding multiple columns", call. = FALSE)
    }
    .args[["input_cols"]] <- cast_nullable_string(.args[["input_cols"]])
    .args[["output_cols"]] <- cast_nullable_string(.args[["output_cols"]])
    estimator <- spark_pipeline_stage(
      x, "org.apache.spark.ml.feature.OneHotEncoder",
      input_col = .args[["input_cols"]], output_col = .args[["output_cols"]], uid = .args[["uid"]]
    ) %>%
      invoke("setDropLast", .args[["drop_last"]]) %>%
      new_ml_one_hot_encoder()
  }

  estimator
}

#' @export
ft_one_hot_encoder.ml_pipeline <- function(x, input_cols = NULL, output_cols = NULL, handle_invalid = "error",
                                           drop_last = TRUE, uid = random_string("one_hot_encoder_"), ...) {
  if(is_required_spark(spark_connection(x), "3.0.0")) {
    stage <- ft_one_hot_encoder.spark_connection(
      x = spark_connection(x),
      input_cols = input_cols,
      output_cols = output_cols,
      handle_invalid = handle_invalid,
      drop_last = drop_last,
      uid = uid,
      ...
    )
  } else {
    stage <- ft_one_hot_encoder.spark_connection(
      x = spark_connection(x),
      input_cols = input_cols,
      output_cols = output_cols,
      drop_last = drop_last,
      uid = uid,
      ...
    )
  }

  ml_add_stage(x, stage)
}

#' @export
ft_one_hot_encoder.tbl_spark <- function(x, input_cols = NULL, output_cols = NULL, handle_invalid = "error",
                                         drop_last = TRUE, uid = random_string("one_hot_encoder_"), ...) {
  if(is_required_spark(spark_connection(x), "3.0.0")) {
    stage <- ft_one_hot_encoder.spark_connection(
      x = spark_connection(x),
      input_cols = input_cols,
      output_cols = output_cols,
      handle_invalid = handle_invalid,
      drop_last = drop_last,
      uid = uid,
      ...
    )
  } else {
    stage <- ft_one_hot_encoder.spark_connection(
      x = spark_connection(x),
      input_cols = input_cols,
      output_cols = output_cols,
      drop_last = drop_last,
      uid = uid,
      ...
    )
  }

  if (is_ml_transformer(stage)) {
    ml_transform(stage, x)
  } else {
    ml_fit_and_transform(stage, x)
  }
}

new_ml_one_hot_encoder <- function(jobj) {
  if (is_required_spark(jobj, "3.0.0")) {
    one_hot_encoder <- new_ml_estimator(jobj, class = "ml_one_hot_encoder")
  } else {
    one_hot_encoder <- new_ml_transformer(jobj, class = "ml_one_hot_encoder")
  }

  one_hot_encoder
}

new_ml_one_hot_encoder_model <- function(jobj) {
  spark_require_version(spark_connection(jobj), "3.0.0")
  new_ml_transformer(
    jobj,
    category_size = invoke(jobj, "categorySize"),
    class = "ml_one_hot_encoder_model"
  )
}

validator_ml_one_hot_encoder <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["drop_last"]] <- cast_scalar_logical(.args[["drop_last"]])
  .args[["handle_invalid"]] <- cast_choice(
    .args[["handle_invalid"]], c("error", "skip", "keep")
  )

  .args
}
