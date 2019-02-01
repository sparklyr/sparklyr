extract_model_metrics <- function(object, metric_names, new_names) {
  sapply(metric_names, function(x) ml_summary(object, x, allow_null = TRUE)) %>%
    t() %>%
    fix_data_frame(newnames = new_names)
}

#' @export
tidy.ml_model <- function(x, ...) {
  stop("'tidy()' not yet supported for ",
       setdiff(class(x), "ml_model"))
}

#' @export
augment.ml_model <- function(x, ...) {
  stop("'augment()' not yet supported for ",
       setdiff(class(x), "ml_model"))
}

#' @export
glance.ml_model <- function(x, ...) {
  stop("'glance()' not yet supported for ",
       setdiff(class(x), "ml_model"))
}

# this function provides broom::augment() for
# supervised models
#' @importFrom rlang syms
broom_augment_supervised <- function(x, newdata = NULL, ...) {

  # if the user doesn't provide a new data, this funcion will
  # use the training set
  if (is.null(newdata)){
    newdata <- x$dataset
  }

  if(inherits(x, "ml_model_classification")) {
    # for classification
    vars <- c(dplyr::tbl_vars(newdata), "predicted_label")

    ml_predict(x, newdata) %>%
      dplyr::select(!!!syms(vars)) %>%
      dplyr::rename(.predicted_label = !!rlang::sym("predicted_label"))

  } else {
    # for regression
    vars <- c(dplyr::tbl_vars(newdata), "prediction")

    ml_predict(x, newdata) %>%
      dplyr::select(!!!syms(vars)) %>%
      dplyr::rename(.prediction = !!rlang::sym("prediction"))
  }

}

# copied from broom to remove dependency

# strip rownames from a data frame
unrowname <- function(x) {
  rownames(x) <- NULL
  x
}

fix_data_frame <- function(x, newnames = NULL, newcol = "term") {
  if (!is.null(newnames) && length(newnames) != ncol(x)) {
    stop("newnames must be NULL or have length equal to number of columns")
  }

  if (all(rownames(x) == seq_len(nrow(x)))) {
    # don't need to move rownames into a new column
    ret <- data.frame(x, stringsAsFactors = FALSE)
    if (!is.null(newnames)) {
      colnames(ret) <- newnames
    }
  }
  else {
    ret <- data.frame(
      ...new.col... = rownames(x),
      unrowname(x),
      stringsAsFactors = FALSE
    )
    colnames(ret)[1] <- newcol
    if (!is.null(newnames)) {
      colnames(ret)[-1] <- newnames
    }
  }
  tibble::as_tibble(ret)
}
