#' Start Livy
#'
#' Starts the livy service.
#'
#' @param version The version of \samp{livy} to use.
#' @param sparkVersion The version of \samp{spark} to connect to.
#'
#' @rdname livy_service
#' @export
livy_service_start <- function(version = NULL, sparkVersion = NULL) {
  env <- unlist(list(
    "SPARK_HOME" = spark_home_dir(version = sparkVersion)
  ))

  livyStart <- file.path(livy_home_dir(version = version), "bin/livy-server")

  if (.Platform$OS.type == "unix") {
    system2("chmod", c("744", livyStart))
  }

  withr::with_envvar(env, {
    system2(
      livyStart,
      wait = FALSE
    )
  })
}

#' Stops Livy
#'
#' Stops the running instances of the livy service.
#'
#' @rdname livy_service
#' @export
livy_service_stop <- function() {
  if (.Platform$OS.type != "unix") {
    stop("Unsupported command in this platform")
  }

  system2("kill", c("`jps | grep \"LivyServer\" | cut -d \" \" -f 1`"), wait = TRUE)
}
