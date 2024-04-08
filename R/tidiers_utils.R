extract_model_metrics <- function(object, metric_names, new_names) {
  sapply(metric_names, function(x) ml_summary(object, x, allow_null = TRUE)) %>%
    t() %>%
    fix_data_frame(newnames = new_names)
}

#' @export
tidy.ml_model <- function(x, ...) {
  stop(
    "'tidy()' not yet supported for ",
    setdiff(class(x), "ml_model")
  )
}

#' @export
augment.ml_model <- function(x, ...) {
  stop(
    "'augment()' not yet supported for ",
    setdiff(class(x), "ml_model")
  )
}

#' @export
glance.ml_model <- function(x, ...) {
  stop(
    "'glance()' not yet supported for ",
    setdiff(class(x), "ml_model")
  )
}

# this function provides broom::augment() for
# supervised models
#' @importFrom rlang syms
broom_augment_supervised <- function(x, newdata = NULL, ...) {

  # if the user doesn't provide a new data, this function will
  # use the training set
  if (is.null(newdata)) newdata <- x$dataset

  preds <- ml_predict(x, newdata)

  orig_vars <- as.character(dplyr::tbl_vars(newdata))
  preds_vars <- as.character(dplyr::tbl_vars(preds))

  rd <- lapply(preds_vars, function(x) ifelse(any(x == orig_vars), "", x))
  rd1 <- as.character(rd)[as.character(rd) != ""]

  if(any(rd1 == "predicted_label")) {
    p_name <- "predicted_label"
    p_new <- ".predicted_label"
  } else {
    p_name <- "prediction"
    p_new <- ".prediction"
  }

  preds_sel <- dplyr::select(preds, !!!syms(c(orig_vars, p_name)))
  rename(preds_sel, !! p_new := !! p_name)
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
  dplyr::as_tibble(ret)
}

# Checks for newdata argument in parsnip models
check_newdata <- function(...) {
  if (any(names(rlang::enquos(...)) == "newdata"))
    rlang::abort("Did you mean to use `new_data` instead of `newdata`?")
}
