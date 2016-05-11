#' @export
#' @rname spark-connection
connect <- function(master = "local", appName = "rspark") {
  setup_local()
  spark_api_start(master, appName)
}

#' @export
#' @rname spark-connection
connection_log <- function(con, n = 10) {
  log <- file(con$outputFile)
  lines <- readLines(log)
  close(log)

  lines <- tail(lines, n = n)

  cat(paste(lines, collapse = "\n"))
}

#' @export
#' @rname spark-connection
connection_ui <- function(con) {
  # Started SparkUI at http://172.16.158.1:4040

  log <- file(con$outputFile)
  lines <- readLines(log)
  close(log)

  lines <- head(lines, n = 200)

  ui_line <- grep("Started SparkUI at ", lines, perl=TRUE, value=TRUE)
  if (length(ui_line) > 0) {
    matches <- regexpr("http://.*", ui_line, perl=TRUE)
    match <-regmatches(ui_line, matches)
    if (length(match) > 0) {
      browseURL(match)
    }
  }
}
