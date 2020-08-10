#' Extracts data associated with a Spark ML model
#'
#' @param object a Spark ML model
#' @return A tbl_spark
#' @export
ml_model_data <- function(object) {
  sdf_register(object$data)
}

possibly_null <- function(.f) purrr::possibly(.f, otherwise = NULL)

#' @export
predict.ml_model_classification <- function(object,
                                            newdata = ml_model_data(object),
                                            ...) {
  ml_predict(object, newdata) %>%
    sdf_read_column("predicted_label")
}

#' @export
predict.ml_model_regression <- function(
                                        object, newdata = ml_model_data(object), ...) {
  prediction_col <- ml_param(object$model, "prediction_col")

  ml_predict(object, newdata) %>%
    sdf_read_column(prediction_col)
}

#' @export
fitted.ml_model_prediction <- function(object, ...) {
  prediction_col <- object$model %>%
    ml_param("prediction_col")
  object %>%
    ml_predict() %>%
    dplyr::pull(!!rlang::sym(prediction_col))
}

#' @export
residuals.ml_model <- function(object, ...) {
  stop("'residuals()' not supported for ", class(object)[[1L]])
}

#' Model Residuals
#'
#' This generic method returns a Spark DataFrame with model
#' residuals added as a column to the model training data.
#'
#' @param object Spark ML model object.
#' @param ... additional arguments
#'
#' @rdname sdf_residuals
#'
#' @export
sdf_residuals <- function(object, ...) {
  UseMethod("sdf_residuals")
}

read_spark_vector <- function(jobj, field) {
  object <- invoke(jobj, field)
  invoke(object, "toArray")
}

read_spark_matrix <- function(jobj, field = NULL) {
  object <- if (rlang::is_null(field)) jobj else invoke(jobj, field)
  nrow <- invoke(object, "numRows")
  ncol <- invoke(object, "numCols")
  data <- invoke(object, "toArray")
  matrix(data, nrow = nrow, ncol = ncol)
}

ml_short_type <- function(x) {
  jobj_class(spark_jobj(x))[1]
}

spark_dense_matrix <- function(sc, mat) {
  if (is.null(mat)) {
    return(mat)
  }
  invoke_new(
    sc, "org.apache.spark.ml.linalg.DenseMatrix", dim(mat)[1L], dim(mat)[2L],
    as.list(mat)
  )
}

spark_dense_vector <- function(sc, vec) {
  if (is.null(vec)) {
    return(vec)
  }
  invoke_static(
    sc, "org.apache.spark.ml.linalg.Vectors", "dense",
    as.list(vec)
  )
}

spark_sql_column <- function(sc, col, alias = NULL) {
  jobj <- invoke_new(sc, "org.apache.spark.sql.Column", col)
  if (!is.null(alias)) {
    jobj <- invoke(jobj, "alias", alias)
  }
  jobj
}

make_stats_arranger <- function(fit_intercept) {
  if (fit_intercept) {
    function(x) {
      force(x)
      c(tail(x, 1), head(x, length(x) - 1))
    }
  } else {
    identity
  }
}
