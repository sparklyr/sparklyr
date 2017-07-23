spark_worker_apply <- function(sc) {
  hostContextId <- worker_invoke_method(sc, FALSE, "Handler", "getHostContext")
  worker_log("retrieved worker context id ", hostContextId)

  context <- structure(
    class = c("spark_jobj", "shell_jobj"),
    list(
      id = hostContextId,
      connection = sc
    )
  )

  worker_log("retrieved worker context")

  bundlePath <- worker_invoke(context, "getBundlePath")
  if (nchar(bundlePath) > 0) {
    worker_log("using bundle ", bundlePath)

    if (file.exists(bundlePath)) {
      worker_log("found local bundle exists, skipping extraction")
    }
    else {
      bundleName <- basename(bundlePath)

      workerRootDir <- worker_invoke_static(sc, "org.apache.spark.SparkFiles", "getRootDirectory")
      sparkBundlePath <- file.path(workerRootDir, bundleName)

      if (!file.exists(sparkBundlePath)) {
        stop("failed to find bundle under SparkFiles root directory")
      }

      worker_log("updated .libPaths with bundle packages")
      unbundlePath <- core_spark_apply_unbundle(sparkBundlePath, workerRootDir)

      .libPaths(unbundlePath)
      worker_log("updated .libPaths with bundle packages")
    }
  }

  grouped_by <- worker_invoke(context, "getGroupBy")
  grouped <- !is.null(grouped_by)
  if (grouped) worker_log("working over grouped data")

  length <- worker_invoke(context, "getSourceArrayLength")
  worker_log("found ", length, " rows")

  groups <- worker_invoke(context, "getSourceArraySeq")
  worker_log("retrieved ", length(groups), " rows")

  closureRaw <- worker_invoke(context, "getClosure")
  closure <- unserialize(closureRaw)

  closureRLangRaw <- worker_invoke(context, "getClosureRLang")
  if (length(closureRLangRaw) > 0) {
    worker_log("found rlang closure")
    closureRLang <- spark_worker_rlang_unserialize()
    if (!is.null(closureRLang)) {
      closure <- closureRLang(closureRLangRaw)
      worker_log("created rlang closure")
    }
  }

  columnNames <- worker_invoke(context, "getColumns")

  if (!grouped) groups <- list(list(groups))

  all_results <- NULL

  for (group_entry in groups) {
    # serialized groups are wrapped over single lists
    data <- group_entry[[1]]

    df <- do.call(rbind.data.frame, data)
    colnames(df) <- columnNames[1: length(colnames(df))]

    g <- if (grouped) df[[grouped_by]][[1]] else NULL

    worker_log("computing closure")
    result <- switch (as.character(length(formals(closure))),
      "0" = closure(),
      "1" = closure(df),
      "2" = closure(df, g)
    )
    worker_log("computed closure")

    if (!identical(class(result), "data.frame")) {
      worker_log("data.frame expected but ", class(result), " found")
      result <- data.frame(result)
    }

    all_results <- rbind(all_results, result)
  }

  all_data <- lapply(1:nrow(all_results), function(i) as.list(all_results[i,]))
  worker_invoke(context, "setResultArraySeq", all_data)
  worker_log("updated ", length(data), " rows")

  spark_split <- worker_invoke(context, "finish")
  worker_log("finished apply")
}

spark_worker_rlang_unserialize <- function() {
  rlang_unserialize <- core_get_package_function("rlang", "bytes_unserialise")
  if (is.null(rlang_unserialize))
    core_get_package_function("rlanglabs", "bytes_unserialise")
  else
    rlang_unserialize
}
