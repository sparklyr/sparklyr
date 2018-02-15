spark_compile_embedded_sources <- function() {
  worker_files <- dir("R", full.names = TRUE, pattern = "worker|core")
  rlines <- unlist(lapply(worker_files, function(e) readLines(e)))
  rlines <- c(rlines, "do.call(spark_worker_main, as.list(commandArgs(trailingOnly = TRUE)))")

  lines <- c(
    "package sparklyr",
    "",
    "class Sources {",
    "  def sources: String = \"\"\"",
    rlines,
    "    \"\"\"",
    "}"
  )

  writeLines(lines, file.path("java", "spark-1.6.0", "sources.scala"))
}
