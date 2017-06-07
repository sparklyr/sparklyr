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

  length <- worker_invoke(context, "getSourceArrayLength")
  worker_log("found ", length, " rows")

  data <- worker_invoke(context, "getSourceArraySeq")
  worker_log("retrieved ", length(data), " rows")

  closureRaw <- worker_invoke(context, "getClosure")
  closure <- unserialize(closureRaw)

  columnNames <- worker_invoke(context, "getColumns")

  df <- do.call(rbind.data.frame, data)
  colnames(df) <- columnNames[1: length(colnames(df))]

  worker_log("computing closure")
  result <- closure(df)
  worker_log("computed closure")

  if (!identical(class(result), "data.frame")) {
    worker_log("data.frame expected but ", class(result), " found")
    result <- data.frame(result)
  }

  data <- apply(result, 1, as.list)

  worker_invoke(context, "setResultArraySeq", data)
  worker_log("updated ", length(data), " rows")

  spark_split <- worker_invoke(context, "finish")
  worker_log("finished apply")
}
