spark_gen_embedded_sources <- function(
    output = file.path("java", "embedded_sources.R")) {
  worker_files <- sort(dir("R", full.names = TRUE, pattern = "worker|core"))
  lines <- c(
    unlist(lapply(worker_files, function(e) readLines(e))),
    "do.call(spark_worker_main, as.list(commandArgs(trailingOnly = TRUE)))"
  )
  writeLines(lines, output)
}
