print_newline <- function() {
  cat("", sep = "\n")
}

ml_model_print_call <- function(model) {
  printf("Call: %s\n", paste(format(model$.call), collapse = " "))
  invisible(model$.call)
}

ml_model_print_residuals <- function(model,
                                     residuals.header = "Residuals") {

  residuals <- model$.model %>%
    invoke("summary") %>%
    invoke("residuals")

  # randomly sample residuals and produce quantiles based on
  # sample to avoid slowness in Spark's 'percentile_approx()'
  # implementation
  count <- invoke(residuals, "count")
  limit <- 1E5
  isApproximate <- count > limit
  column <- invoke(residuals, "columns")[[1]]

  values <- if (isApproximate) {
    fraction <- limit / count
    residuals %>%
      invoke("sample", FALSE, fraction) %>%
      sdf_read_column(column) %>%
      quantile()
  } else {
    residuals %>%
      sdf_read_column(column) %>%
      quantile()
  }
  names(values) <- c("Min", "1Q", "Median", "3Q", "Max")

  header <- if (isApproximate)
    paste(residuals.header, "(approximate):")
  else
    paste(residuals.header, ":", sep = "")

  cat(header, sep = "\n")
  print(values, digits = max(3L, getOption("digits") - 3L))
  invisible(values)
}

#' @importFrom stats coefficients quantile
ml_model_print_coefficients <- function(model) {

  coef <- coefficients(model)

  cat("Coefficients:", sep = "\n")
  print(coef)
  invisible(coef)
}

ml_model_print_coefficients_detailed <- function(model) {

  # extract relevant columns for stats::printCoefmat call
  # (protect against routines that don't provide standard
  # error estimates, etc)
  columns <- c("coefficients", "standard.errors", "t.values", "p.values")
  values <- as.list(model[columns])
  for (value in values)
    if (is.null(value))
      return(ml_model_print_coefficients(model))

  matrix <- do.call(base::cbind, values)
  colnames(matrix) <- c("Estimate", "Std. Error", "t value", "Pr(>|t|)")

  cat("Coefficients:", sep = "\n")
  stats::printCoefmat(matrix)
}

ml_model_print_centers <- function(model) {

  centers <- model$centers
  if (is.null(centers))
    return()

  cat("Cluster centers:", sep = "\n")
  print(model$centers)

}
