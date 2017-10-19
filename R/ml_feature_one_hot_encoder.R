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
ft_one_hot_encoder <- function(
  x, input_col, output_col, drop_last = TRUE,
  uid = random_string("one_hot_encoder_"), ...) {
  UseMethod("ft_one_hot_encoder")
}

#' @export
ft_one_hot_encoder.spark_connection <- function(
  x, input_col, output_col, drop_last = TRUE,
  uid = random_string("one_hot_encoder_"), ...) {

  ml_ratify_args()

  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.OneHotEncoder",
                             input_col, output_col, uid) %>%
    invoke("setDropLast", drop_last)

  new_ml_one_hot_encoder(jobj)
}

#' @export
ft_one_hot_encoder.ml_pipeline <- function(
  x, input_col, output_col, drop_last = TRUE,
  uid = random_string("one_hot_encoder_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_one_hot_encoder.tbl_spark <- function(
  x, input_col, output_col, drop_last = TRUE,
  uid = random_string("one_hot_encoder_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)
}

new_ml_one_hot_encoder <- function(jobj) {
  new_ml_transformer(jobj, subclass = "ml_one_hot_encoder")
}
