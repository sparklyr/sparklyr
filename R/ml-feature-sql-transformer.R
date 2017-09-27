# SQLTransformer

#' @export
ft_sql_transformer <- function(x, statement,
                               uid = random_string("sql_transformer_"), ...) {
  UseMethod("ft_sql_transformer")
}

#' @export
ft_sql_transformer.spark_connection <- function(
  x, statement,
  uid = random_string("sql_transformer_"), ...) {

  ml_validate_args()
  jobj <- invoke_new(x, "org.apache.spark.ml.feature.SQLTransformer", uid) %>%
    invoke("setStatement", statement)

  new_ml_transformer(jobj)
}

#' @export
ft_sql_transformer.ml_pipeline <- function(
  x, statement,
  uid = random_string("sql_transformer_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_sql_transformer.tbl_spark <- function(
  x, statement,
  uid = random_string("sql_transformer_"), ...) {
  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)
}

# dplyr transformer

ft_extract_sql <- function(x) {
  table_name <- paste0("`",
                       x[["ops"]][["x"]][1],
                       "`")
  dbplyr::sql_render(x) %>%
    (function(x) gsub(table_name, "__THIS__", x))
}

#' @export
ft_dplyr_transformer <- function(
  x, tbl,
  uid = random_string("dplyr_transformer_"), ...) {
  UseMethod("ft_dplyr_transformer")
}

#' @export
ft_dplyr_transformer.spark_connection <- function(
  x, tbl,
  uid = random_string("dplyr_transformer_"), ...) {

  if (!identical(class(tbl)[1], "tbl_spark")) stop("'tbl' must be a Spark table")
  ft_sql_transformer(x, ft_extract_sql(tbl), uid = uid)
}

#' @export
ft_dplyr_transformer.ml_pipeline <- function(
  x, tbl,
  uid = random_string("dplyr_transformer_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_dplyr_transformer.tbl_spark <- function(
  x, tbl,
  uid = random_string("dplyr_transformer_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)
}
