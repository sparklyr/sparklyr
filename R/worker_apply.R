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

  data <- lapply(data, function(e) {
    names(e) <- columnNames
    e
  })

  data <- if (length(formals(closure)) > 0)
    lapply(data, closure)
  else
    lapply(data, function(e) {
      closure()
    })

  if (!identical(typeof(data[[1]]), "list")) {
    data <- lapply(data, function(e) list(e))
  }

  worker_invoke(context, "setResultArraySeq", data)
  worker_log("updated ", length(data), " rows")

  spark_split <- worker_invoke(context, "finish")
  worker_log("finished apply")
}
