#' Spark ML -- Transform, fit, and predict methods (ml_ interface)
#'
#' Methods for transformation, fit, and prediction. These are mirrors of the corresponding \link{sdf-transform-methods}.
#'
#' @param x A \code{ml_estimator}, \code{ml_transformer} (or a list thereof), or \code{ml_model} object.
#' @param dataset A \code{tbl_spark}.
#' @template roxlate-ml-dots
#'
#' @details These methods are
#'
#' @return When \code{x} is an estimator, \code{ml_fit()} returns a transformer whereas \code{ml_fit_and_transform()} returns a transformed dataset. When \code{x} is a transformer, \code{ml_transform()} and \code{ml_predict()} return a transformed dataset. When \code{ml_predict()} is called on a \code{ml_model} object, additional columns (e.g. probabilities in case of classification models) are appended to the transformed output for the user's convenience.
#'
#' @name ml-transform-methods
NULL

#' @rdname ml-transform-methods
#' @export
is_ml_transformer <- function(x) inherits(x, "ml_transformer")

#' @rdname ml-transform-methods
#' @export
is_ml_estimator <- function(x) inherits(x, "ml_estimator")

#' @rdname ml-transform-methods
#' @export
ml_fit <- function(x, dataset, ...) {
  if (!is_ml_estimator(x))
    stop("'ml_fit()' is only applicable to 'ml_estimator' objects")

  spark_jobj(x) %>%
    invoke("fit", spark_dataframe(dataset)) %>%
    ml_call_constructor()
}

#' @rdname ml-transform-methods
#' @export
ml_transform <- function(x, dataset, ...) {
  UseMethod("ml_transform")
}

#' @export
ml_transform.default <- function(x, dataset, ...) {
  stop("Transformers must be 'ml_transformer' objects.")
}

#' @export
ml_transform.list <- function(x, dataset, ...) {
  if (!all(sapply(x, is_ml_transformer)))
    stop("Transformers must be 'ml_transformer' objects.")
  sdf <- spark_dataframe(dataset)

  transforms <- x %>%
    lapply(spark_jobj)

  result_sdf <- Reduce(
    function(dataset, transformer) invoke(transformer, "transform", dataset),
    transforms,
    init = sdf
  )

  sdf_register(result_sdf)
}

#' @export
ml_transform.ml_transformer <- function(x, dataset, ...) {
  sdf <- spark_dataframe(dataset)
  spark_jobj(x) %>%
    invoke("transform", sdf) %>%
    sdf_register()
}

#' @rdname ml-transform-methods
#' @export
ml_fit_and_transform <- function(x, dataset, ...) {
  if (!is_ml_estimator(x))
    stop("'ml_fit_and_transform()' is only applicable to 'ml_estimator' objects")
  sdf <- spark_dataframe(dataset)
  spark_jobj(x)%>%
    invoke("fit", sdf) %>%
    invoke("transform", sdf) %>%
    sdf_register()
}

#' @rdname ml-transform-methods
#' @export
ml_predict <- function(x, dataset, ...) {
  UseMethod("ml_predict")
}

#' @export
ml_predict.default <- function(x, dataset, ...) {
  ml_transform(x, dataset)
}

#' @export
ml_predict.ml_model_regression <- function(x, dataset, ...) {
  # when dataset is not supplied, attempt to use original dataset
  if (missing(dataset) || rlang::is_null(dataset))
    dataset <- x$dataset

  cols <- x$model %>%
    ml_params(c("prediction_col", "variance_col"),
              allow_null = TRUE) %>%
    Filter(length, .) %>%
    unlist(use.names = FALSE)

  x$pipeline_model %>%
    ml_transform(dataset) %>%
    select(!!!rlang::syms(c(tbl_vars(dataset), cols)))
}
#' @rdname ml-transform-methods
#' @param probability_prefix String used to prepend the class probability output columns.
#' @export
ml_predict.ml_model_classification <- function(
  x, dataset,
  probability_prefix = "probability_", ...
) {
  sc <- spark_connection(x$model)
  probability_prefix <- cast_string(probability_prefix)

  if (missing(dataset) || rlang::is_null(dataset))
    dataset <- x$dataset

  predictions <- x$pipeline_model %>%
    ml_transform(dataset)

  probability_col <- ml_param(x$model, "probability_col", allow_null = TRUE)
  if (rlang::is_null(probability_col)) {
    predictions
  } else {
    index_labels <- spark_sanitize_names(
      x$index_labels %||% (seq_len(x$model$num_classes) - 1L),
      sc$config
    )
    sdf_separate_column(
      predictions, probability_col,
      paste0(probability_prefix, index_labels)
    )
  }
}

#' @export
ml_predict.ml_model_clustering <- function(x, dataset, ...) {
  # when dataset is not supplied, attempt to use original dataset
  if (missing(dataset) || rlang::is_null(dataset))
    dataset <- x$dataset

  x$pipeline_model %>%
    ml_transform(dataset)
}

#' @export
ml_predict.ml_model_recommendation <- function(x, dataset, ...) {
  # when dataset is not supplied, attempt to use original dataset
  if (missing(dataset) || rlang::is_null(dataset))
    dataset <- x$dataset

  x$pipeline_model %>%
    ml_transform(dataset)
}

#' Spark ML -- Transform, fit, and predict methods (sdf_ interface)
#'
#' Deprecated methods for transformation, fit, and prediction. These are mirrors of the corresponding \link{ml-transform-methods}.
#'
#' @param x A \code{tbl_spark}.
#' @param model A \code{ml_transformer} or a \code{ml_model} object.
#' @param transformer A \code{ml_transformer} object.
#' @param estimator A \code{ml_estimator} object.
#' @param ... Optional arguments passed to the corresponding \code{ml_} methods.
#'
#' @return \code{sdf_predict()}, \code{sdf_transform()}, and \code{sdf_fit_and_transform()} return a transformed dataframe whereas \code{sdf_fit()} returns a \code{ml_transformer}.
#'
#' @name sdf-transform-methods
NULL

#' @rdname sdf-transform-methods
#' @export
sdf_predict <- function(x, model, ...) {
  .Deprecated("ml_predict")
  UseMethod("sdf_predict")
}

#' @export
sdf_predict.default <- function(x, model, ...) {
  ml_predict(model, sdf_register(x), ...)
}

#' @rdname sdf-transform-methods
#' @export
sdf_transform <- function(x, transformer, ...) {
  .Deprecated("ml_transform")
  ml_transform(transformer, sdf_register(x))
}

#' @rdname sdf-transform-methods
#' @export
sdf_fit <- function(x, estimator, ...) {
  .Deprecated("ml_fit")
  ml_fit(estimator, sdf_register(x))
}

#' @rdname sdf-transform-methods
#' @export
sdf_fit_and_transform <- function(x, estimator, ...) {
  .Deprecated("ml_fit_and_transform")
  ml_fit_and_transform(estimator, sdf_register(x))
}

