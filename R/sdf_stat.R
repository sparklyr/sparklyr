#' Cross Tabulation
#'
#' Builds a contingency table at each combination of factor levels.
#'
#' @param x A Spark DataFrame
#' @param col1 The name of the first column. Distinct items will make the first item of each row.
#' @param col2 The name of the second column. Distinct items will make the column names of the DataFrame.
#' @return A DataFrame containing the contingency table.
#' @export
sdf_crosstab <- function(x, col1, col2) {
  col1 <- cast_string(col1)
  col2 <- cast_string(col2)

  x %>%
    spark_dataframe() %>%
    invoke("%>%", list("stat"), list("crosstab", col1, col2)) %>%
    sdf_register()
}
