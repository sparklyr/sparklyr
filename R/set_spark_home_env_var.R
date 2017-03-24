#' Set the SPARK_HOME environment variable
#' 
#' Set the \code{SPARK_HOME} environment variable. This slightly speeds up some 
#' operations, including the connection time.
#' @param path A string containing the path to the installation location of
#' Spark. If \code{NULL}, the path to the most latest Spark/Hadoop versions is
#' used. 
#' @param verbose Logical. Should the function explain what is it doing?
#' @return The function is mostly invoked for the side-effect of setting the 
#' \code{SPARK_HOME} environment variable. It also returns \code{TRUE} if the 
#' environment was successfully set, nad \code{FALSE} otherwise.
#' @examples
#' \dontrun{
#' # Not run due to side-effects
#' set_spark_home_env_var()
#' }
#' @export
set_spark_home_env_var <- function(path = NULL, verbose = is.null(path)) {
  force(verbose)
  if(is.null(path)) {
    path <- spark_install_find()$sparkVersionDir
  }
  if(verbose) {
    message("Setting SPARK_HOME environment variable to ", path)
  }
  Sys.setenv(SPARK_HOME = path)
}
