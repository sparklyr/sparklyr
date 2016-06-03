#' Computes kmeans from a dplyr source
#' @export
#' @param x A dplyr source.
#' @param centers Number of centers to compute.
#' @param iter.max Maximum number of iterations used to compute kmeans.
spark_mllib_kmeans <- function(x, centers, iter.max = 10) {
  db <- x$src
  scon <- spark_scon(db)

  # TODO: Do we really want a formula interface? Why not just
  # let the caller pass in a list of column names?
  formula <- "~ ."

  sql <- as.character(sql_render(sql_build(x, con = db$con), con = db$con))

  # Extract a Spark DataFrame from the x object
  if (inherits(x, "tbl_spark")) {
    x <- spark_invoke(
      spark_sql_or_hive(spark_api(x$src)),
      "sql",
      sql
    )
  }

  if (!inherits(x, "jobj"))
    stop("'x' should be a 'jobj' (referencing a Spark data set)")

  kmeans_wrapper <- spark_invoke_static(
    scon,

    "org.apache.spark.ml.r.KMeansWrapper",
    "fit",

    x,
    formula,
    as.integer(centers),
    as.integer(iter.max),
    "k-means||"
  )

  # TODO: Return a nicer interface to the KMeans object or just centers?
  centers <- spark_invoke(kmeans_wrapper, "fitted", "centers")
  centers <- spark_collect(centers)
}
