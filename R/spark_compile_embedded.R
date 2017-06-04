#' @export
spark_compile_embedded_sources <- function() {
  worker_files <- dir("R", full.names = TRUE, pattern = "worker")
  rlines <- unlist(lapply(worker_files, function(e) readLines(e)))
  rlines <- gsub("\\\\", "\\\\\\\\", rlines)
  rlines <- gsub("\\\"", "\\\\\"", rlines)
  rlines <- c(rlines, "spark_worker_main(commandArgs(trailingOnly = TRUE)[1])")

  lines <- c(
    "package sparklyr",
    "",
    "object WorkerSources {",
    "  def sources: String = \"\" +",
    paste("    \"", rlines, "\\n\" +", sep = ""),
    "    \"\"",
    "}"
  )

  writeLines(lines, file.path("java", "spark-1.5.2", "workersources.scala"))
}
