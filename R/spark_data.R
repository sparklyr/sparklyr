spark_data_build_types <- function(sc, columns) {
  names <- names(columns)
  fields <- lapply(
    names,
    function(name) {
      if (is.list(columns[[name]])) {
        struct <- spark_data_build_types(sc, columns[[name]])
        invoke_static(
          sc,
          "sparklyr.SQLUtils",
          "createStructField",
          name,
          struct
        )
      } else {
        invoke_static(
          sc,
          "sparklyr.SQLUtils",
          "createStructField",
          name,
          columns[[name]][[1]],
          TRUE
        )
      }
    }
  )

  invoke_static(sc, "sparklyr.SQLUtils", "createStructType", fields)
}

#' Retrieve a Spark DataFrame
#'
#' This S3 generic is used to access a Spark DataFrame object (as a Java
#' object reference) from an \R object.
#'
#' @param x An \R object wrapping, or containing, a Spark DataFrame.
#' @param ... Optional arguments; currently unused.
#' @return A \code{\link{spark_jobj}} representing a Java object reference
#'   to a Spark DataFrame.
#'
#' @export
spark_dataframe <- function(x, ...) {
  UseMethod("spark_dataframe")
}

#' @export
spark_dataframe.default <- function(x, ...) {
  stop(
    "Unable to retrieve a Spark DataFrame from object of class ",
    paste(class(x), collapse = " "),
    call. = FALSE
  )
}

#' @export
spark_dataframe.spark_jobj <- function(x, ...) {
  x
}


# nocov start

spark_schema_from_rdd <- function(sc, rdd, column_names) {
  columns_typed <- length(names(column_names)) > 0

  if (columns_typed) {
    schema <- spark_data_build_types(sc, column_names)
    return(schema)
  }

  sampleRows <- rdd %>%
    invoke(
      "take",
      cast_scalar_integer(
        spark_config_value(sc$config, "sparklyr.apply.schema.infer", 10)
      )
    )

  map_special_types <- list(
    date = "date",
    posixct = "timestamp",
    posixt = "timestamp"
  )

  colTypes <- NULL
  lapply(sampleRows, function(r) {
    row <- r %>% invoke("toSeq")

    if (is.null(colTypes)) {
      colTypes <<- replicate(length(row), "character")
    }

    lapply(seq_along(row), function(colIdx) {
      colVal <- row[[colIdx]]
      lowerClass <- tolower(class(colVal)[[1]])
      if (lowerClass %in% names(map_special_types)) {
        colTypes[[colIdx]] <<- map_special_types[[lowerClass]]
      } else if (!is.na(colVal) && !is.null(colVal)) {
        colTypes[[colIdx]] <<- typeof(colVal)
      }
    })
  })

  if (any(sapply(colTypes, is.null))) {
    stop("Failed to infer column types, please use explicit types.")
  }

  fields <- lapply(seq_along(colTypes), function(idx) {
    name <- if (idx <= length(column_names)) {
      column_names[[idx]]
    } else {
      paste0("X", idx)
    }

    invoke_static(
      sc,
      "sparklyr.SQLUtils",
      "createStructField",
      name,
      colTypes[[idx]],
      TRUE
    )
  })

  invoke_static(
    sc,
    "sparklyr.SQLUtils",
    "createStructType",
    fields
  )
}

# nocov end
