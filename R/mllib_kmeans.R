spark_mllib_kmeans <- function(scon, x, centers, iter.max = 10) {

  # TODO: Do we really want a formula interface? Why not just
  # let the caller pass in a list of column names?
  formula <- "~ ."

  # Extract a Spark DataFrame from the x object
  # TODO: Use formula interface to determine what we extract?
  if (inherits(x, "tbl_spark")) {
    x <- spark_invoke(
      spark_api_create_sql_context(scon),
      "sql",
      paste("SELECT * FROM", spark_table_name(x))
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

  # TODO: Return a nicer interface to the KMeans object.
  return(kmeans_wrapper)
}
