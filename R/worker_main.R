log_file <- file.path("~", "spark", basename(tempfile(fileext = ".log")))

log <- function(message) {
  write(paste0(message, "\n"), file = log_file, append = TRUE)
  cat("sparkworker:", message, "\n")
}

log("sparklyr worker starting")
log("sparklyr worker finished")
