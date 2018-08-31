extract_model_metrics <- function(object, metric_names, new_names) {
  sapply(metric_names, function(x) ml_summary(object, x, allow_null = TRUE)) %>%
    t() %>%
    broom::fix_data_frame(newnames = new_names)
}

#' @export
tidy.ml_model <- function(x, ...) {
  stop(paste0("'tidy()' not yet supported for ",
              setdiff(class(x), "ml_model"))
  )
}

#' @export
augment.ml_model <- function(x, ...) {
  stop(paste0("'augment()' not yet supported for ",
              setdiff(class(x), "ml_model"))
  )
}

#' @export
glance.ml_model <- function(x, ...) {
  stop(paste0("'glance()' not yet supported for ",
              setdiff(class(x), "ml_model"))
  )
}

# this function provides broom::augment() for
# supervised models
#' @importFrom rlang syms
broom_augment_supervised <- function(x, newdata = NULL,
                                     ...){

  # if the user doesn't provide a new data, this funcion will
  # use the training set
  if (is.null(newdata)){
    newdata <- x$dataset
  }

  if(any(class(x) == "ml_model_classification")){
    # for classification
    vars <- c(dplyr::tbl_vars(newdata), "predicted_label")

    ml_predict(x, newdata) %>%
      dplyr::select(!!!syms(vars)) %>%
      dplyr::rename(.predicted_label = !!"predicted_label")

  } else {
    # for regression
    vars <- c(dplyr::tbl_vars(newdata), "prediction")

    ml_predict(x, newdata) %>%
      dplyr::select(!!!syms(vars)) %>%
      dplyr::rename(.prediction = !!"prediction")
  }

}
