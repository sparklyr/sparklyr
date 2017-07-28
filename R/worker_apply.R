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

      unbundlePath <- worker_spark_apply_unbundle(sparkBundlePath, workerRootDir)

      .libPaths(unbundlePath)
      worker_log("updated .libPaths with bundle packages")
    }
  }

  grouped_by <- worker_invoke(context, "getGroupBy")
  grouped <- !is.null(grouped_by) && length(grouped_by) > 0
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

    closure_params <- length(formals(closure))
    closure_args <- c(
      list(df),
      as.list(
        if (nrow(df) > 0)
          lapply(grouped_by, function(group_by_name) df[[group_by_name]][[1]])
        else
          NULL
      )
    )[0:closure_params]

    worker_log("computing closure")
    result <- do.call(closure, closure_args)
    worker_log("computed closure")

    if (!identical(class(result), "data.frame")) {
      worker_log("data.frame expected but ", class(result), " found")
      result <- data.frame(result)
    }

    if (grouped) {
      new_column_values <- lapply(grouped_by, function(grouped_by_name) df[[grouped_by_name]][[1]])
      names(new_column_values) <- grouped_by

      result <- do.call("cbind", list(new_column_values, result))
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

#' Extracts a bundle of dependencies required by \code{spark_apply()}
#'
#' @param bundle_path Path to the bundle created using \code{core_spark_apply_bundle()}
#' @param base_path Base path to use while extracting bundles
#'
#' @keywords internal
#' @export
worker_spark_apply_unbundle <- function(bundle_path, base_path) {
  extractPath <- file.path(base_path, core_spark_apply_unbundle_path())

  if (!dir.exists(extractPath)) dir.create(extractPath, recursive = TRUE)

  if (length(dir(extractPath)) == 0) {
    worker_log("found that the unbundle path is empty, extracting:", extractPath)
    system2("tar", c("-xf", bundle_path, "-C", extractPath))
  }

  extractPath
}
