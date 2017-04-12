package SparkWorker

object Embedded {
  def sources: String = "" +
    "log_file <- file.path(\"~\", \"spark\", basename(tempfile(fileext = \".log\")))\n" +
    "\n" +
    "log <- function(message) {\n" +
    "  write(paste0(message, \"\\n\"), file = log_file, append = TRUE)\n" +
    "  cat(\"sparkworker:\", message, \"\\n\")\n" +
    "}\n" +
    "\n" +
    "log(\"sparklyr worker starting\")\n" +
    "log(\"sparklyr worker finished\")\n" +
    ""
}
