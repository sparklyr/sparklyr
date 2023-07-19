
testthat_tbl <- function(name, data = NULL, repartition = 0L) {
  sc <- testthat_spark_connection()

  tbl <- tryCatch(dplyr::tbl(sc, name), error = identity)
  if (inherits(tbl, "error")) {
    if (is.null(data)) data <- eval(as.name(name), envir = parent.frame())
    tbl <- dplyr::copy_to(sc, data, name = name, repartition = repartition)
  }

  tbl
}


sdf_query_plan <- function(x, plan_type = c("optimizedPlan", "analyzed")) {
  plan_type <- match.arg(plan_type)

  x %>%
    spark_dataframe() %>%
    invoke("queryExecution") %>%
    invoke(plan_type) %>%
    invoke("toString") %>%
    strsplit("\n") %>%
    unlist()
}



get_default_args <- function(fn, exclude = NULL) {
  formals(fn) %>%
    (function(x) x[setdiff(names(x), c(exclude, c("x", "uid", "...", "formula")))])
}

param_filter_version <- function(args, min_version, params) {
  sc <- testthat_spark_connection()
  if (spark_version(sc) < min_version) {
    args[params] <- NULL
  }
  args
}

param_add_version <- function(args, min_version, ...) {
  sc <- testthat_spark_connection()
  if (spark_version(sc) >= min_version) {
    c(args, list(...))
  } else {
    args
  }
}

output_file <- function(filename) file.path("output", filename)

is_testing_databricks_connect <- function() {
  Sys.getenv("TEST_DATABRICKS_CONNECT") == "true"
}

random_table_name <- function(prefix) {
  paste0(prefix, paste0(floor(runif(10, 0, 10)), collapse = ""))
}

get_test_data_path <- function(file_name) {
  if (Sys.getenv("TEST_DATABRICKS_CONNECT") == "true") {
    test_data_path <- paste0(Sys.getenv("DBFS_DATA_PATH"), "/", file_name)
  } else {
    test_data_path <- file.path(normalizePath(getwd()), "data", file_name)
    if(!file.exists(test_data_path)) {
      test_data_path <- normalizePath(test_path("data", file_name))
    }
  }

  test_data_path
}

# Helper method to launch a local proxy listening on `proxy_port` and forwarding
# TCP packets to `dest_port`
# This method will return an opaque handle with a finalizer that will stop the
# proxy process once called
local_tcp_proxy <- function(proxy_port, dest_port) {
  pid <- system2(
    "bash",
    args = c(
      "-c",
      paste0(
        "'socat tcp-l:",
        as.integer(proxy_port),
        ",fork,reuseaddr tcp:localhost:",
        as.integer(dest_port),
        " >/dev/null 2>&1 & disown; echo $!'"
      )
    ),
    stdout = TRUE
  )
  wait_for_svc("local_tcp_proxy", proxy_port, timeout_s = 10)

  handle <- structure(
    new.env(parent = emptyenv()),
    class = "local_tcp_proxy_handle"
  )
  reg.finalizer(
    handle,
    function(x) {
      system2("pkill", args = c("-P", pid))
      system2("kill", args = pid)
    },
    onexit = TRUE
  )

  handle
}
