#' Define a Spark dependency
#' 
#' Define a Spark dependency, which is a specification of JAR files and/or
#' Spark packages that are required for an extension.
#' 
#' @param jars Character vector of full paths to JAR files
#' @param packages Character vector of Spark package names
#' 
#' @return An object of class \code{spark_dependency}
#'
#' @export
spark_dependency <- function(jars = NULL, packages = NULL) {
  structure(class = "spark_dependency",
    jars = jars,
    packages = packages
  )
}
