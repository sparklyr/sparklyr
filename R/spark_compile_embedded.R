spark_compile_embedded_sources <- function() {
  worker_files <- dir("R", full.names = TRUE, pattern = "worker|core")
  rlines <- unlist(lapply(worker_files, function(e) readLines(e)))
  rlines <- gsub("\\\\", "\\\\\\\\", rlines)
  rlines <- gsub("\\\"", "\\\\\"", rlines)
  rlines <- c(rlines, "if (identical(Sys.getenv('SPARKLYR_WORKER_PACKAGE'), 'TRUE'))")
  rlines <- c(rlines, "  do.call(sparklyr:::spark_worker_main, as.list(commandArgs(trailingOnly = TRUE)))")
  rlines <- c(rlines, "else")
  rlines <- c(rlines, "  do.call(spark_worker_main, as.list(commandArgs(trailingOnly = TRUE)))")

  lines <- c(
    "package sparklyr",
    "",
    "object Sources {",
    "  def sources: String = \"\" +",
    paste("    \"", rlines, "\\n\" +", sep = ""),
    "    \"\"",
    "}"
  )

  writeLines(lines, file.path("java", "spark-1.6.0", "sources.scala"))
}
