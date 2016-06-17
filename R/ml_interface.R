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

sdf_summarize <- function(df, columns = NULL) {
  sdf <- as_spark_dataframe(df)
  if (is.null(columns))
    columns <- dplyr::tbl_vars(sdf)
  
  # register table (drop when we're done)
  name <- random_string()
  ctx <- spark_invoke(sdf, "sqlContext")
  spark_invoke(sdf, "registerTempTable", name)
  on.exit(spark_invoke(ctx, "dropTempTable", name))
  
  # compute summary on each of the set columns
  summaries <- lapply(columns, function(column) {
    
    template <- paste(
      "SELECT",
      paste(
        "AVG(%s) as Average",
        "VARIANCE(%s) as Variance",
        "MIN(%s) as Minimum",
        "PERCENTILE_APPROX(%s, 0.25) AS Q1",
        "PERCENTILE_APPROX(%s, 0.50) AS Median",
        "PERCENTILE_APPROX(%s, 0.75) AS Q3",
        "MAX(%s) as Maximum",
        sep = ", "
      ),
      "FROM", name
    )
    
    sql <- gsub("%s", column, template, fixed = TRUE)
    spark_invoke(ctx, "sql", sql)
    
  })
  
  result <- lapply(summaries, function(summary) {
    summary %>%
      spark_invoke("collect") %>%
      `[[`(1)
  })
  names(result) <- columns
  
  result
}
