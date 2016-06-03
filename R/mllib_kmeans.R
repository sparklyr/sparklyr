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
  if (!inherits(x, "tbl_spark")) {
    stop("The kmeans source is expected to be compatible with dplyr.")
  }

  sqlResult <- spark_invoke(
    spark_sql_or_hive(spark_api(x$src)),
    "sql",
    sql
  )

  rddRow <- spark_invoke(sqlResult, "rdd")

  rddVector <- spark_invoke_static(scon, "utils", "schemaRddToVectorRdd", rddRow)

  kMeansModel <- spark_invoke_static(
    scon,

    "org.apache.spark.mllib.clustering.KMeans",
    "train",

    rddVector,
    as.integer(centers),
    as.integer(iter.max)
  )

  # TODO: Return a nicer interface to the KMeans object or just centers?
  spark_invoke(kMeansModel, "clusterCenters")
}
