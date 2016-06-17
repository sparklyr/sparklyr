ml_model <- function(class, model, ...) {
  object <- list(..., .model = model)
  class(object) <- c(
    paste("ml_model", class, sep = "_"),
    "ml_model"
  )
  object
}

#' @export
spark_connection.ml_model <- function(x, ...) {
  spark_connection(x$.model)
}

ml_model_print_residuals_summary <- function(model) {
  scon <- spark_connection(model)

  residuals <- model$.model %>%
    spark_invoke("summary") %>%
    spark_invoke("residuals")

  name <- random_string()
  spark_invoke(residuals, "registerTempTable", name)

  ctx <- spark_invoke(residuals, "sqlContext")

  sql <- paste(
    "SELECT",
    paste(
      "MIN(residuals) as Min",
      "PERCENTILE_APPROX(residuals, 0.25) AS Q1",
      "PERCENTILE_APPROX(residuals, 0.50) AS Median",
      "PERCENTILE_APPROX(residuals, 0.75) AS Q3",
      "MAX(residuals) as Max",
      sep = ", "
    ),
    "FROM", name
  )

  summary <-
    spark_invoke(ctx, "sql", sql) %>%
    spark_invoke("collect") %>%
    unlist(recursive = FALSE) %>%
    as.numeric()

  names(summary) <- c("Min", "1Q", "Median", "3Q", "Max")

  cat("Residuals:", sep = "\n")
  print(summary)

  invisible(summary)
}
