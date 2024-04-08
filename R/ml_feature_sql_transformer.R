#' Feature Transformation -- SQLTransformer
#'
#' Implements the transformations which are defined by SQL statement. Currently we
#'   only support SQL syntax like 'SELECT ... FROM __THIS__ ...' where '__THIS__' represents
#'   the underlying table of the input dataset. The select clause specifies the
#'   fields, constants, and expressions to display in the output, it can be any
#'   select clause that Spark SQL supports. Users can also use Spark SQL built-in
#'   function and UDFs to operate on these selected columns.
#'
#' @template roxlate-ml-feature-transformer
#' @param statement A SQL statement.
#'
#' @rdname sql-transformer
#' @export
ft_sql_transformer <- function(x, statement = NULL,
                               uid = random_string("sql_transformer_"), ...) {
  check_dots_used()
  UseMethod("ft_sql_transformer")
}

ml_sql_transformer <- ft_sql_transformer

#' @export
ft_sql_transformer.spark_connection <- function(x, statement = NULL,
                                                uid = random_string("sql_transformer_"), ...) {
  .args <- list(
    statement = statement,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_sql_transformer()

  jobj <- invoke_new(
    x, "org.apache.spark.ml.feature.SQLTransformer",
    .args[["uid"]]
  ) %>%
    jobj_set_param("setStatement", .args[["statement"]])

  new_ml_sql_transformer(jobj)
}

#' @export
ft_sql_transformer.ml_pipeline <- function(x, statement = NULL,
                                           uid = random_string("sql_transformer_"), ...) {
  stage <- ft_sql_transformer.spark_connection(
    x = spark_connection(x),
    statement = statement,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_sql_transformer.tbl_spark <- function(x, statement = NULL,
                                         uid = random_string("sql_transformer_"), ...) {
  stage <- ft_sql_transformer.spark_connection(
    x = spark_connection(x),
    statement = statement,
    uid = uid,
    ...
  )
  ml_transform(stage, x)
}

new_ml_sql_transformer <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_sql_transformer")
}

validator_ml_sql_transformer <- function(.args) {
  if (inherits(.args[["statement"]], "sql")) {
    .args[["statement"]] <- as.character(.args[["statement"]])
  }

  .args[["statement"]] <- cast_nullable_string(.args[["statement"]])
  .args[["uid"]] <- cast_string(.args[["uid"]])
  .args
}
