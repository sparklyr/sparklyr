#' Feature Transformation -- OneHotEncoder (Transformer)
#'
#' One-hot encoding maps a column of label indices to a column of binary
#' vectors, with at most a single one-value. This encoding allows algorithms
#' which expect continuous features, such as Logistic Regression, to use
#' categorical features. Typically, used with  \code{ft_string_indexer()} to
#' index a column first.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @param drop_last Whether to drop the last category. Defaults to \code{TRUE}.
#'
#' @export
ft_one_hot_encoder <- function(x, input_col = NULL, output_col = NULL,
                               drop_last = TRUE, uid = random_string("one_hot_encoder_"), ...) {
  UseMethod("ft_one_hot_encoder")
}

#' @export
ft_one_hot_encoder.spark_connection <- function(x, input_col = NULL, output_col = NULL,
                                                drop_last = TRUE, uid = random_string("one_hot_encoder_"), ...) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    drop_last = drop_last,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    ml_validator_one_hot_encoder()

  jobj <- ml_new_transformer(
    x, "org.apache.spark.ml.feature.OneHotEncoder",
    input_col = .args[["input_col"]], output_col = .args[["output_col"]], uid = .args[["uid"]]
  ) %>%
    invoke("setDropLast", .args[["drop_last"]])

  new_ml_one_hot_encoder(jobj)
}

#' @export
ft_one_hot_encoder.ml_pipeline <- function(x, input_col = NULL, output_col = NULL,
                                           drop_last = TRUE, uid = random_string("one_hot_encoder_"), ...) {
  stage <- ft_one_hot_encoder.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    drop_last = drop_last,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_one_hot_encoder.tbl_spark <- function(x, input_col = NULL, output_col = NULL,
                                         drop_last = TRUE, uid = random_string("one_hot_encoder_"), ...) {
  stage <- ft_one_hot_encoder.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    drop_last = drop_last,
    uid = uid,
    ...
  )
  ml_transform(stage, x)
}

new_ml_one_hot_encoder <- function(jobj) {
  new_ml_transformer(jobj, subclass = "ml_one_hot_encoder")
}

ml_validator_one_hot_encoder <- function(.args) {
  .args <- validate_args_transformer(.args) %>%
    ml_backwards_compatibility(list(drop.last = "drop_last"))
  .args[["drop_last"]] <- cast_scalar_logical(.args[["drop_last"]])
  .args
}
