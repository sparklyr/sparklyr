#' @export
ml_fit <- function(x, data, ...) {
  UseMethod("ml_fit")
}

#' @export
ml_fit.ml_pipeline <- function(x, data, ...) {
  jobj <- x$.jobj %>%
    invoke("fit", spark_dataframe(data))

  new_ml_pipeline_model(jobj)
}

#' @export
ml_fit.ml_estimator <- function(x, data, ...) {
  jobj <- x$.jobj %>%
    invoke("fit", spark_dataframe(data))

  new_ml_transformer(jobj)
}

#' @export
ml_fit.ml_predictor <- function(x, data, ...) {
  jobj <- x$.jobj %>%
    invoke("fit", spark_dataframe(data))

  new_ml_prediction_model(jobj)
}

#' @export
ml_fit.ml_count_vectorizer <- function(x, data, ...) {
  jobj <- x$.jobj %>%
    invoke("fit", spark_dataframe(data))

  new_ml_count_vectorizer_model(jobj)
}

#' @export
ml_fit.ml_transformer <- function(x, data, ...) {
  stop("cannot invoke 'fit' on transformers; 'ml_fit()' should be used with estimators")
}

#' @export
ml_transform <- function(x, data, ...) {
  UseMethod("ml_transform")
}

#' @export
ml_transform.ml_transformer <- function(x, data, ...) {
  sdf <- spark_dataframe(data)
  x$.jobj %>%
    invoke("transform", sdf) %>%
    sdf_register()
}

#' @export
ml_transform.ml_estimator <- function(x, data, ...) {
  stop("cannot invoke 'transform' on estimators; 'ml_transform()' should be used with transformers")
}

#' @export
ml_fit_and_transform <- function(x, data, ...) {
  UseMethod("ml_fit_and_transform")
}

#' @export
ml_fit_and_transform.ml_estimator <- function(x, data, ...) {
  sdf <- spark_dataframe(data)
  x$.jobj%>%
    invoke("fit", sdf) %>%
    invoke("transform", sdf) %>%
    sdf_register()
}

#' @export
ml_fit_and_transform.ml_transformer <- function(x, data, ...) {
  stop("cannot invoke 'fit' on transformers; 'ml_fit_and_transform()' should be used with estimators")
}
