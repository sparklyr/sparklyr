extract_estimate_statistics <- function(object, stat_names, new_names) {
  stat_names %>%
    lapply(function(x) ml_summary(object, x, allow_null = TRUE)) %>%
    as.data.frame() %>%
    broom::fix_data_frame(newnames = new_names)
}

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
