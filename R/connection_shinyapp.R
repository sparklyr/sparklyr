# nocov start

#' A Shiny app that can be used to construct a \code{spark_connect} statement
#'
#' @importFrom rstudioapi showQuestion
#'
#' @export
#' @keywords internal
connection_spark_shinyapp <- function() {
  if (!"shiny" %in% installed.packages()) {
    install_shiny <- showQuestion("Shiny Required", "The 'shiny' package is not installed, install?", ok = "Install")
    if (identical(install_shiny, TRUE)) {
      install_command <- get("install.packages")
      install_command("shiny")
    }

    if (!"shiny" %in% installed.packages()) {
      stop("The 'shiny' package is not installed, please install and retry.")
    }
  }

  shinyAppDir <- get("shinyAppDir", envir = asNamespace("shiny"))
  shinyAppDir(system.file("rstudio/shinycon", package = "sparklyr"))
}

# nocov end
