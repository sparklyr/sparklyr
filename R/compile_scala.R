#' @export
update_sources_class <- function() {
  rlines <- readLines("R/worker_main.R")
  rlines <- gsub("\\\"", "\\\\\"", rlines)
  rlines <- gsub("\\\\n", "\\\\\\\\n", rlines)

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
