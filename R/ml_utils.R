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
#' @importFrom dplyr arrange
#' @importFrom lazyeval interp
predict.ml_model <- function(object,
                             newdata = object$data,
                             ...)
{
  # 'sdf_predict()' does not necessarily return a data set with the same row
  # order as the input data; generate a unique id and re-order based on this
  id <- random_string("id_")
  sdf <- newdata %>%
    sdf_with_unique_id(id) %>%
    spark_dataframe()

  # perform prediction
  params <- object$model.parameters
  predicted <- sdf_predict(object, sdf, ...)

  # re-order based on id column
  arranged <- arrange(predicted, id)

  # read column
  column <- sdf_read_column(arranged, "prediction")

  # re-map label ids back to actual labels
  if (is.character(params$labels) && is.numeric(column))
    column <- params$labels[column + 1]

  column
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


reorder_first <- function(vector, name) {
  if (is.null(vector))
    return(vector)

  nm <- names(vector)
  if (is.null(nm) || !name %in% nm)
    return(vector)

  ordered <- c(name, base::setdiff(nm, name))
  vector[ordered]
}

intercept_first <- function(vector) {
  reorder_first(vector, "(Intercept)")
}

read_spark_vector <- function(jobj, field) {
  object <- invoke(jobj, field)
  invoke(object, "toArray")
}

read_spark_matrix <- function(jobj, field) {
  object <- invoke(jobj, field)
  nrow <- invoke(object, "numRows")
  ncol <- invoke(object, "numCols")
  data <- invoke(object, "toArray")
  matrix(data, nrow = nrow, ncol = ncol)
}

ml_short_type <- function(x) {
  strsplit(x$type, "\\.") %>%
    rlang::flatten_chr() %>%
    dplyr::last()
}
