spark_apply_packages <- function(packages) {
  db <- Sys.getenv("sparklyr.apply.packagesdb")
  if (nchar(db) == 0) {
    if (!exists("availablePackagesChache", envir = .globals)) {
      db <- tryCatch(
        {
          available.packages()
        },
        error = function(e) {
          warning(
            "Failed to run 'available.packages()', using offline connection? ",
            "See '?spark_apply' for details."
          )
          NULL
        }
      )

      assign("availablePackagesChache", db, envir = .globals)
    }
    else {
      db <- get("availablePackagesChache", envir = .globals)
    }
  }

  if (is.null(db)) {
    TRUE
  } else {
    deps <- tools::package_dependencies(packages, db = db, recursive = TRUE)
    names(deps) <- NULL
    unique(c(unlist(deps), packages))
  }
}

spark_apply_bundle_path <- function() {
  file.path(tempdir(), "packages")
}

spark_apply_bundle_file <- function(packages, base_path, session_id) {
  file.path(
    base_path,
    if (isTRUE(packages)) {
      do.call(paste, as.list(c("packages", session_id, "tar", sep = ".")))
    } else {
      paste(
        substr(
          digest::digest(
            paste(packages, collapse = "-"),
            algo = "sha256"
          ),
          start = 1,
          stop = 7
        ),
        "tar",
        sep = "."
      )
    }
  )
}

#' Create Bundle for Spark Apply
#'
#' Creates a bundle of packages for \code{spark_apply()}.
#'
#' @param packages List of packages to pack or \code{TRUE} to pack all.
#' @param base_path Base path used to store the resulting bundle.
#' @param session_id An optional ID string to include in the bundle file name to allow the bundle to be session-specific
#'
#' @export
spark_apply_bundle <- function(packages = TRUE, base_path = getwd(), session_id = NULL) {
  # If session_id is not provied use a random string to avoid file name collision.
  session_id <- session_id %||% uuid::UUIDgenerate(use.time = TRUE)

  packages <- if (is.character(packages)) spark_apply_packages(packages) else packages

  packagesTar <- spark_apply_bundle_file(packages, base_path, session_id)

  if (!dir.exists(spark_apply_bundle_path())) {
    dir.create(spark_apply_bundle_path(), recursive = TRUE)
  }

  args <- c(
    "-chf",
    packagesTar,
    if (isTRUE(packages)) {
      lapply(.libPaths(), function(e) {
        c("-C", e, ".")
      }) %>% unlist()
    } else {
      added_packages <- list()
      lapply(.libPaths(), function(e) {
        sublib_packages <- Filter(
          Negate(is.null),
          lapply(packages, function(p) {
            if (file.exists(file.path(e, p)) && !p %in% added_packages) {
              added_packages <<- c(added_packages, p)
              p
            }
          })
        ) %>% unlist()

        if (length(sublib_packages) > 0) c("-C", e, sublib_packages) else NULL
      }) %>% unlist()
    }
  )

  if (!file.exists(packagesTar)) {
    system2("tar", args)
  }

  packagesTar
}

spark_apply_packages_is_bundle <- function(packages) {
  is.character(packages) && length(packages) == 1 && grepl("\\.tar$", packages)
}

get_spark_apply_bundle_path <- function(sc, packages) {
  bundle_path <- ""
  if (spark_apply_packages_is_bundle(packages)) {
    bundle_path <- packages
  } else if (isTRUE(packages) || is.character(packages)) {
    bundle_base <- spark_apply_bundle_path()
    bundle_path <- spark_apply_bundle_file(packages, bundle_base, sc$sessionId)
    if (!file.exists(bundle_path)) {
      bundle_path <- spark_apply_bundle(packages, bundle_base, sc$sessionId)
    }

    if (!is.null(bundle_path)) {
      bundle_was_added <- file.exists(
        invoke_static(
          sc,
          "org.apache.spark.SparkFiles",
          "get",
          basename(bundle_path)
        )
      )

      if (!bundle_was_added) {
        spark_context(sc) %>% invoke("addFile", bundle_path)
      }
    }
  }

  bundle_path
}
