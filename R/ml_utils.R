#' Extracts data associated with a Spark ML model
#'
#' @param object a Spark ML model
#' @return A tbl_spark
#' @export
ml_model_data <- function(object) {
  sdf_register(object$data)
}

try_null <- function(expr) {
  tryCatch(expr, error = function(e) NULL)
}

#' @export
predict.ml_model_classification <- function(object,
                                            newdata = ml_model_data(object),
                                            ...)
{
  ml_predict(object, newdata) %>%
    sdf_read_column("predicted_label")
}

#' @export
predict.ml_model_regression <- function(
  object, newdata = ml_model_data(object), ...
) {
  prediction_col <- ml_param(object$model, "prediction_col")

  ml_predict(object, newdata) %>%
    sdf_read_column(prediction_col)
}

#' @export
fitted.ml_model <- function(object, ...) {

  predictions <- object$.model %>%
    invoke("summary") %>%
    invoke("predictions")

  id <- object$model.parameters$id
  object$data %>%
    invoke("join", predictions, as.list(id)) %>%
    sdf_read_column("prediction")
}

#' @export
residuals.ml_model <- function(object, ...) {
  stop(paste0("'residuals()' not yet supported for ",
              setdiff(class(object), "ml_model"))
  )
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
