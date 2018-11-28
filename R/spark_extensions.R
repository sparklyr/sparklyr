

#' Register a Package that Implements a Spark Extension
#'
#' Registering an extension package will result in the package being
#' automatically scanned for spark dependencies when a connection to Spark is
#' created.
#'
#' @param package The package(s) to register.
#'
#' @note Packages should typically register their extensions in their
#'   \code{.onLoad} hook -- this ensures that their extensions are registered
#'   when their namespaces are loaded.
#'
#' @export
register_extension <- function(package) {
  .globals$extension_packages <- c(.globals$extension_packages, package)
}

#' @rdname register_extension
#' @export
registered_extensions <- function() {
  .globals$extension_packages
}


#' Define a Spark dependency
#'
#' Define a Spark dependency consisting of a set of custom JARs and Spark packages.
#'
#' @param jars Character vector of full paths to JAR files
#' @param packages Character vector of Spark packages names
#'
#' @return An object of type `spark_dependency`
#'
#' @export
spark_dependency <- function(jars = NULL, packages = NULL) {
  structure(class = "spark_dependency", list(
    jars = jars,
    packages = packages
  ))
}

spark_dependencies_from_extensions <- function(spark_version, scala_version, extensions) {

  jars <- character()
  packages <- character()

  lapply(extensions, function(extension) {
    dependencies <- spark_dependencies_from_extension(spark_version, scala_version, extension)
    lapply(dependencies, function(dependency) {
      jars <<- c(jars, dependency$jars)
      packages <<- c(packages, dependency$packages)
    })
  })

  list(
    jars = jars,
    packages = packages
  )
}

spark_dependencies_from_extension <- function(spark_version, scala_version, extension) {

  # attempt to find the function
  spark_dependencies <- tryCatch({
    get("spark_dependencies", asNamespace(extension), inherits = FALSE)
  },
  error = function(e) {
    stop("spark_dependencies function not found within ",
         "extension package ", extension, call. = FALSE)
  }
  )

  # reduce the spark_version to just major and minor versions
  spark_version <- package_version(spark_version)
  spark_version <- paste(spark_version$major, spark_version$minor, sep = '.')
  spark_version <- numeric_version(spark_version)

  # call the function
  dependency <- spark_dependencies(spark_version = spark_version,
                                   scala_version = scala_version)

  # if it's just a single dependency then wrap it in a list
  if (inherits(dependency, "spark_dependency"))
    dependency <- list(dependency)

  # return it
  dependency
}

sparklyr_jar_path <- function(spark_version) {
  if (spark_version < "2.0")
    scala_version <- "2.10"
  else
    scala_version <- "2.11"
  spark_major_minor <- spark_version[1, 1:2]

  exact_jar <- sprintf("sparklyr-%s-%s.jar", spark_major_minor, scala_version)
  all_jars <- dir(system.file("java",package = "sparklyr"), pattern = "sparklyr")

  if (exact_jar %in% all_jars) {
    system.file(file.path("java", exact_jar), package = "sparklyr")
  } else if (spark_version > "1.6") {
    # Spark is backwards compatible so we can use a new version with the latest jar
    latest_jar <- sort(all_jars, decreasing = T)[[1]]
    system.file(file.path("java", latest_jar), package = "sparklyr")
  } else {
    ""
  }
}

# sparklyr's own declared dependencies
spark_dependencies <- function(spark_version, scala_version) {
  spark_dependency(jars = sparklyr_jar_path(spark_version))
}
