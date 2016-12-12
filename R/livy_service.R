#' Start Livy
#'
#' Starts the livy service.
#'
#' @param version The version of \samp{livy} to use.
#' @param spark_version The version of \samp{spark} to connect to.
#'
#' @rdname livy_service
#' @export
livy_service_start <- function(version = NULL, spark_version = NULL) {
  env <- unlist(list(
    "SPARK_HOME" = spark_home_dir(version = spark_version)
  ))

  # warn if the user attempts to use livy 0.2.0 with Spark >= 2.0.0
  if (!identical(spark_version, NULL)) {
    spark_version <- ensure_scalar_character(spark_version)
    if (version == "0.2.0" &&
        numeric_version(spark_version) >= "2.0.0") {
      stopf("livy %s is not compatible with Spark (>= %s)", version, "2.0.0")
    }
  }

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
