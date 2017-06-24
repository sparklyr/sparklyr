create_hive_context.spark_shell_connection <- function(sc) {
  if (spark_version(sc) >= "2.0.0")
    create_hive_context_v2(sc)
  else
    shell_create_hive_context_v1(sc)
}

shell_create_hive_context_v1 <- function(sc) {

  # get spark_context
  ctx <- spark_context(sc)

  # attempt to create hive_context
  hive_context <- tryCatch({
    backend <- invoke_static(sc, "sparklyr.Shell", "getBackend")
    invoke(
      backend,
      "getOrCreateHiveContext",
      ctx
    )},
    error = function(e) {
      warning(e$message)
      NULL
    }
  )

  # if we failed then create a SqlContext instead
  if (is.null(hive_context)) {

    warning("Failed to create Hive context, falling back to SQL. Some operations, ",
            "like window-functions, will not work")

    jsc <- invoke_static(
      sc,
      "org.apache.spark.api.java.JavaSparkContext",
      "fromSparkContext",
      ctx
    )

    hive_context <- invoke_static(
      sc,
      "org.apache.spark.sql.api.r.SQLUtils",
      "createSQLContext",
      jsc
    )
  }

  # apply configuration
  params <- connection_config(sc, "spark.sql.")
  apply_config(params, hive_context, "setConf", "spark.sql.")

  # return hive_context
  hive_context
}
