#' @export
update_sources_class <- function() {
  worker_files <- dir("R", full.names = TRUE, pattern = "worker")
  rlines <- unlist(lapply(worker_files, function(e) readLines(e)))
  rlines <- gsub("\\\\", "\\\\\\\\", rlines)
  rlines <- gsub("\\\"", "\\\\\"", rlines)
  rlines <- c(rlines, "spark_worker_main(commandArgs(trailingOnly = TRUE)[2])")

  lines <- c(
    "package SparkWorker",
    "",
    "object Embedded {",
    "  def sources: String = \"\" +",
    paste("    \"", rlines, "\\n\" +", sep = ""),
    "    \"\"",
    "}"
  )

  writeLines(lines, file.path("java", "sources.scala"))
}
