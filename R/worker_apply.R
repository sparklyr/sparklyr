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
    closureRLang <- spark_worker_rlang_unserialize(closureRLangRaw)
    if (!is.null(closureRLang)) {
      closure <- closureRLang
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

spark_worker_rlang_unserialize <- function(closureRaw) {
  rlang_unserialize <- NULL
  if (exists("serialise_bytes", envir = asNamespace("rlang")))
    rlang_unserialize <- get("bytes_unserialise", envir = asNamespace("rlang"))
  else if (exists("serialise_bytes", envir = asNamespace("rlanglabs")))
    rlang_unserialize <- get("bytes_unserialise", envir = asNamespace("rlanglabs"))

  if (!is.null(rlang_unserialize)) {
    rlang_unserialize(closureRaw)
  }
  else {
    NULL
  }
}
