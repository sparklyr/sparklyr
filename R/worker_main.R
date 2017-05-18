spark_worker_main <- function(sessionId) {
  log_file <- file.path("~", "spark", basename(tempfile(fileext = ".log")))

  log <- function(message) {
    write(paste0(message, "\n"), file = log_file, append = TRUE)
    cat("sparkworker:", message, "\n")
  }

  log("sparklyr worker starting")

  sc <- spark_worker_connect(sessionId)

  log("sparklyr worker connected")

  spark_worker_apply(sc)

  log("sparklyr worker finished")
}
