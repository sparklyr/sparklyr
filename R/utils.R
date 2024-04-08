#' It lets the package know if it should test a particular functionality or not
#' @details
#' It expects a boolean to be returned. If TRUE, the corresponding test will be
#' skipped. If FALSE the test will be conducted.
#'
#' @param sc Spark connection
#' @param test_name The name of the test
#' @export
spark_integ_test_skip <- function(sc, test_name) {
  UseMethod("spark_integ_test_skip")
}

#' @export
spark_integ_test_skip.default <- function(sc, test_name) {
  FALSE
}

is.installed <- function(package) {
  is.element(package, installed.packages()[, 1])
}

utils_starts_with <- function(lhs, rhs) {
  if (nchar(lhs) < nchar(rhs)) {
    return(FALSE)
  }
  identical(substring(lhs, 1, nchar(rhs)), rhs)
}

aliased_path <- function(path) {
  home <- path.expand("~/")
  if (utils_starts_with(path, home)) {
    path <- file.path("~", substring(path, nchar(home) + 1))
  }
  path
}

transpose_list <- function(list) {
  do.call(Map, c(c, list, USE.NAMES = FALSE))
}

#' Random string generation
#'
#' Generate a random string with a given prefix.
#'
#' @param prefix A length-one character vector.
#' @export
random_string <- function(prefix = "table") {
  paste0(prefix, "_", gsub("-", "_", uuid::UUIDgenerate()))
}

#' Instantiate a Java array with a specific element type.
#'
#' Given a list of Java object references, instantiate an \code{Array[T]}
#' containing the same list of references, where \code{T} is a non-primitive
#' type that is more specific than \code{java.lang.Object}.
#'
#' @param sc A \code{spark_connection}.
#' @param x A list of Java object references.
#' @param element_type A valid Java class name representing the generic type
#'   parameter of the Java array to be instantiated. Each element of \code{x}
#'   must refer to a Java object that is assignable to \code{element_type}.
#'
#' @examples
#' sc <- spark_connect(master = "spark://HOST:PORT")
#'
#' string_arr <- jarray(sc, letters, element_type = "java.lang.String")
#' # string_arr is now a reference to an array of type String[]
#'
#' @export
jarray <- function(sc, x, element_type) {
  cls <- paste0("[L", element_type, ";")
  arr_cls <- invoke_static(sc, "java.lang.Class", "forName", cls)

  j_invoke_static(
    sc, "java.util.Arrays", "copyOf", as.list(x), length(x), arr_cls
  )
}

#' Instantiate a Java float type.
#'
#' Instantiate a \code{java.lang.Float} object with the value specified.
#' NOTE: this method is useful when one has to invoke a Java/Scala method
#' requiring a float (instead of double) type for at least one of its
#' parameters.
#'
#' @param sc A \code{spark_connection}.
#' @param x A numeric value in R.
#'
#' @examples
#' sc <- spark_connect(master = "spark://HOST:PORT")
#'
#' jflt <- jfloat(sc, 1.23e-8)
#' # jflt is now a reference to a java.lang.Float object
#'
#' @export
jfloat <- function(sc, x) {
  j_invoke_new(sc, "java.lang.Float", as.numeric(x))
}

#' Instantiate an Array[Float].
#'
#' Instantiate an \code{Array[Float]} object with the value specified.
#' NOTE: this method is useful when one has to invoke a Java/Scala method
#' requiring an \code{Array[Float]} as one of its parameters.
#'
#' @param sc A \code{spark_connection}.
#' @param x A numeric vector in R.
#'
#' @examples
#' sc <- spark_connect(master = "spark://HOST:PORT")
#'
#' jflt_arr <- jfloat_array(sc, c(-1.23e-8, 0, -1.23e-8))
#' # jflt_arr is now a reference an array of java.lang.Float
#'
#' @export
jfloat_array <- function(sc, x) {
  vals <- lapply(x, function(v) j_invoke_new(sc, "java.lang.Float", v))
  jarray(sc, vals, "java.lang.Float")
}

printf <- function(fmt, ...) {
  cat(sprintf(fmt, ...))
}

spark_require_version <- function(sc, required, module = NULL, required_max = NULL) {

  # guess module based on calling function
  if (is.null(module)) {
    call <- sys.call(sys.parent())
    module <- tryCatch(as.character(call[[1]]), error = function(ex) "")
  }

  # check and report version requirements
  version <- spark_version(sc)
  if (version < required) {
    fmt <- "%s requires Spark %s or higher."
    msg <- sprintf(fmt, module, required, version)
    stop(msg, call. = FALSE)
  } else if (!is.null(required_max)) {
    if (version >= required_max) {
      fmt <- "%s is removed in Spark %s."
      msg <- sprintf(fmt, module, required_max, version)
      stop(msg, call. = FALSE)
    }
  }

  TRUE
}

is_required_spark <- function(x, required_version) {
  UseMethod("is_required_spark")
}

is_required_spark.spark_connection <- function(x, required_version) {
  version <- spark_version(x)
  version >= required_version
}

is_required_spark.spark_jobj <- function(x, required_version) {
  sc <- spark_connection(x)
  is_required_spark(sc, required_version)
}

spark_param_deprecated <- function(param, version = "3.x") {
  warning("The '", param, "' parameter is deprecated in Spark ", version)
}

regex_replace <- function(string, ...) {
  dots <- list(...)
  nm <- names(dots)
  for (i in seq_along(dots)) {
    string <- gsub(nm[[i]], dots[[i]], string, perl = TRUE)
  }
  string
}

spark_sanitize_names <- function(names, config) {
  # Spark 1.6.X has a number of issues with '.'s in column names, e.g.
  #
  #    https://issues.apache.org/jira/browse/SPARK-5632
  #    https://issues.apache.org/jira/browse/SPARK-13455
  #
  # Many of these issues are marked as resolved, but it appears this is
  # a common regression in Spark and the handling is not uniform across
  # the Spark API.

  # sanitize names by default, but opt out with global option
  if (!isTRUE(spark_config_value(config, "sparklyr.sanitize.column.names", TRUE))) {
    return(names)
  }

  # begin transforming names
  oldNames <- newNames <- names

  # use 'iconv' to translate names to ASCII if possible
  newNames <- unlist(lapply(newNames, function(name) {

    # attempt to translate to ASCII
    transformed <- tryCatch(
      iconv(name, to = "ASCII//TRANSLIT"),
      error = function(e) NA
    )

    # on success, return the transformed name
    if (!is.na(transformed)) {
      transformed
    } else {
      name
    }
  }))

  # replace spaces with '_', and discard other characters
  newNames <- regex_replace(
    newNames,
    "^\\s*|\\s*$" = "",
    "[\\s.]+"        = "_",
    "[^\\w_]"     = "",
    "^(\\W)"      = "V\\1"
  )

  # ensure new names are unique
  newNames <- make.unique(newNames, sep = "_")

  # report translations
  verbose <- spark_config_value(
    config,
    c("sparklyr.verbose.sanitize", "sparklyr.sanitize.column.names.verbose", "sparklyr.verbose"),
    FALSE
  )

  if (verbose) {
    changedIdx <- which(oldNames != newNames)
    if (length(changedIdx)) {
      changedOldNames <- oldNames[changedIdx]
      changedNewNames <- newNames[changedIdx]

      nLhs <- max(nchar(changedOldNames))
      nRhs <- max(nchar(changedNewNames))

      lhs <- sprintf(paste("%-", nLhs + 2, "s", sep = ""), shQuote(changedOldNames))
      rhs <- sprintf(paste("%-", nRhs + 2, "s", sep = ""), shQuote(changedNewNames))

      n <- floor(log10(max(changedIdx)))
      index <- sprintf(paste("(#%-", n, "s)", sep = ""), changedIdx)

      msg <- paste(
        "The following columns have been renamed:",
        paste("-", lhs, "=>", rhs, index, collapse = "\n"),
        sep = "\n"
      )

      message(msg)
    }
  }

  newNames
}

# normalizes a path that we are going to send to spark but avoids
# normalizing remote identifiers like hdfs:// or s3n://. note
# that this will take care of path.expand ("~") as well as converting
# relative paths to absolute (necessary since the path will be read by
# another process that has a different current working directory)
spark_normalize_single_path <- function(path) {
  # don't normalize paths that are urls
  parsed <- httr::parse_url(path)
  if (!is.null(parsed$scheme)) {
    path
  } else {
    normalizePath(path, mustWork = FALSE)
  }
}

spark_normalize_path <- function(paths) {
  unname(sapply(paths, spark_normalize_single_path))
}

stopf <- function(fmt, ..., call. = TRUE, domain = NULL) {
  stop(simpleError(
    sprintf(fmt, ...),
    if (call.) sys.call(sys.parent())
  ))
}

warnf <- function(fmt, ..., call. = TRUE, immediate. = FALSE) {
  warning(sprintf(fmt, ...), call. = call., immediate. = immediate.)
}

enumerate <- function(object, f, ...) {
  nm <- names(object)
  result <- lapply(seq_along(object), function(i) {
    f(nm[[i]], object[[i]], ...)
  })
  names(result) <- names(object)
  result
}

path_program <- function(program, fmt = NULL) {
  fmt <- fmt %||% "program '%s' is required but not available on the path"
  path <- Sys.which(program)
  if (!nzchar(path)) {
    stopf(fmt, program, call. = FALSE)
  }
  path
}

infer_active_package_name <- function() {
  root <- package_root()
  dcf <- read.dcf(file.path(root, "DESCRIPTION"), all = TRUE)
  dcf$Package
}

split_chunks <- function(x, chunk_size) {

  # return early when chunk_size > length of vector
  n <- length(x)
  if (n <= chunk_size) {
    return(list(x))
  }

  # compute ranges for subsetting
  starts <- seq(1, n, by = chunk_size)
  ends <- c(seq(chunk_size, n - 1, by = chunk_size), n)

  # apply our subsetter
  mapply(function(start, end) {
    x[start:end]
  }, starts, ends, SIMPLIFY = FALSE, USE.NAMES = FALSE)
}

remove_class <- function(object, class) {
  classes <- attr(object, "class")
  newClasses <- classes[!classes %in% c(class)]

  attr(object, "class") <- newClasses
  object
}

trim_whitespace <- function(strings) {
  gsub("^[[:space:]]*|[[:space:]]*$", "", strings)
}

split_separator <- function(sc) {
  if (inherits(sc, "livy_connection")) {
    list(regexp = "\\|~\\|", plain = "|~|")
  } else {
    list(regexp = "\3", plain = "\3")
  }
}

resolve_fn <- function(fn, ...) {
  if (is.function(fn)) fn(...) else fn
}

is.tbl_spark <- function(x) {
  inherits(x, "tbl_spark")
}

`%<-%` <- function(x, value) {
  dest <- as.character(as.list(substitute(x))[-1])
  if (length(dest) != length(value)) stop("Assignment must contain same number of elements")

  for (i in seq_along(dest)) {
    assign(dest[[i]], value[[i]], envir = sys.frame(which = sys.parent(n = 1)))
  }

  invisible(NULL)
}

sort_named_list <- function(lst, ...) {
  lst[order(names(lst), ...)]
}

# syntax sugar for calling dplyr methods with do.call and a non-trivial variable
# list of args
`%>>%` <- function(x, fn) {
  fn_call <- function(largs) {
    do.call(fn, append(list(x), as.list(largs)))
  }

  fn_call
}

`%@%` <- function(fn, largs) fn(largs)

# syntax sugar for executing a chain of method calls with each call operating on
# the JVM object returned from the previous call
`%>|%` <- function(x, invocations) {
  do.call(invoke, append(list(x, "%>%"), invocations))
}

pcre_to_java <- function(regex) {
  regex %>%
    gsub("\\[:alnum:\\]", "A-Za-z0-9", .) %>%
    gsub("\\[:alpha:\\]", "A-Za-z", .) %>%
    gsub("\\[:ascii:\\]", paste0("\\\\", "x00", "-", "\\\\", "x7F"), .) %>%
    gsub("\\[:blank:\\]", " \\\\t", .) %>%
    gsub("\\[:cntrl:\\]", paste0("\\\\", "x00", "-", "\\\\", "x1F", "\\\\", "x7F"), .) %>%
    gsub("\\[:digit:\\]", "0-9", .) %>%
    gsub("\\[:graph:\\]", paste0("\\\\", "x21", "-", "\\\\", "x7E"), .) %>%
    gsub("\\[:lower:\\]", "a-z", .) %>%
    gsub("\\[:print:\\]", paste0("\\\\", "x20", "-", "\\\\", "x7E"), .) %>%
    gsub("\\[:punct:\\]",
         paste0("\\\\", "x21", "-", "\\\\", "x2F",
                "\\\\", "x3A", "-", "\\\\", "x40",
                "\\\\", "x5B", "-", "\\\\", "x60",
                "\\\\", "x7B", "-", "\\\\", "x7E"),
         .
        ) %>%
    gsub("\\[:space:\\]",
         paste0(" ",
                "\\\\", "t",
                "\\\\", "r",
                "\\\\", "n",
                "\\\\", "v",
                "\\\\", "f"
         ),
         .
        ) %>%
    gsub("\\[:upper:\\]", "A-Z", .) %>%
    gsub("\\[:word:\\]", "A-Za-z0-9_", .) %>%
    gsub("\\[:xdigit:\\]", "0-9a-fA-F", .)
}

# helper method returning a minimal R dataframe containing the same set of
# column names as `sdf` does
replicate_colnames <- function(sdf) {
  columns <- lapply(
    colnames(sdf),
    function(column) {
      v <- list(NA)
      names(v) <- column
      v
    }
  )
  do.call(data.frame, columns)
}

translate_spark_column_types <- function(sdf) {
  type_map <- list(
    BooleanType = "logical",
    ByteType = "integer",
    ShortType = "integer",
    IntegerType = "integer",
    FloatType = "numeric",
    DoubleType = "numeric",
    LongType = "numeric",
    StringType = "character",
    BinaryType = "raw",
    TimestampType = "POSIXct",
    DateType = "Date",
    CalendarIntervalType = "character",
    NullType = "NULL"
  )

  sdf %>%
    sdf_schema() %>%
    lapply(
      function(e) {
        if (e$type %in% names(type_map)) {
          type_map[[e$type]]
        } else if (grepl("^(Array|Struct|Map)Type\\(.*\\)$", e$type)) {
          "list"
        } else if (grepl("^DecimalType\\(.*\\)$", e$type)) {
          "numeric"
        } else {
          "unknown"
        }
      }
    )
}

simulate_vars_spark <- function(x, drop_groups = FALSE) {
  col_types <- translate_spark_column_types(x)

  if (drop_groups) {
    non_group_cols <- setdiff(names(col_types), dplyr::group_vars(x))
    col_types <- col_types[non_group_cols]
  }

  col_types %>%
    lapply(
      function(x) {
        fn <- tryCatch(
          get(paste0("as.", x), envir = parent.frame()),
          error = function(e) {
            NULL
          }
        )

        if (is.null(fn)) {
          list()
        } else {
          fn(NA)
        }
      }
    ) %>%
    dplyr::as_tibble()
}

#' @importFrom tidyselect tidyselect_data_proxy tidyselect_data_has_predicates
#' @export
tidyselect_data_proxy.tbl_spark <- function(x) {
  if(tidyselect_data_has_predicates(x)) {
    simulate_vars_spark(x, FALSE)
  } else {
    NextMethod()
  }
}

#' @export
tidyselect_data_has_predicates.tbl_spark <- function(x) {
  supported <- unlist(options("sparklyr.support.predicates"))
  out <- TRUE
  if(!is.null(supported)) {
    out <- supported
  }
  out
}

# wrapper for download.file()
download_file <- function(...) {
  min_timeout_s <- 300

  # Temporarily set download.file() timeout to 300 seconds if it was
  # previously less than that, and restore the previous timeout setting
  # on exit.
  prev_timeout_s <- getOption("timeout")
  if (prev_timeout_s < min_timeout_s) {
    on.exit(options(timeout = prev_timeout_s))
    options(timeout = min_timeout_s)
  }

  download.file(...)
}

# Infer all R packages that may be required for executing `fn`
infer_required_r_packages <- function(fn) {
  pkgs <- as.data.frame(installed.packages())
  deps <- new.env(hash = TRUE, parent = emptyenv(), size = nrow(pkgs))

  populate_deps <- function(pkg) {
    pkg <- as.character(pkg)

    if (!identical(deps[[pkg]], TRUE)) {
      imm_deps <- pkg %>%
        tools::package_dependencies(db = installed.packages(), recursive = FALSE)
      purrr::map(imm_deps[[1]], ~ populate_deps(.x))
      deps[[pkg]] <- TRUE
    }
  }

  rlang::fn_body(fn) %>%
    globals::walkAST(
      call = function(x) {
        cfn <- rlang::eval_bare(x[[1]])

        for (mfn in list(base::library,
                         base::require,
                         base::requireNamespace,
                         base::loadNamespace)) {
          if (identical(cfn, mfn)) {
            populate_deps(rlang::call_args(match.call(mfn, x))$package)
            return(x)
          }
        }

        if (identical(cfn, base::attachNamespace)) {
          populate_deps(rlang::call_args(match.call(base::attachNamespace, x))$ns)
          return(x)
        }

        ns <- rlang::call_ns(x)
        if (!is.null(ns)) {
          populate_deps(ns)
        } else {
          where <- strsplit(find(rlang::call_name(x)), ":")[[1]]
          if (identical(where[[1]], "package")) {
            populate_deps(where[[2]])
          }
        }

        x
      }
    )

  ls(deps)
}

get_os <- function() {
  if (.Platform$OS.type == "windows") {
    "win"
  } else if (Sys.info()["sysname"] == "Darwin") {
    "mac"
  } else {
    "unix"
  }
}

os_is_windows <- function() {
  get_os() == "win"
}

cast_string <- function(x) {
  vctrs::vec_check_size(x, 1, arg = rlang::caller_arg(x), call = rlang::caller_env())
  vctrs::vec_cast(x, character())
}

cast_scalar_logical <- function(x) {
  vctrs::vec_check_size(x, 1, arg = rlang::caller_arg(x), call = rlang::caller_env())
  vctrs::vec_cast(x, logical())
}

cast_scalar_double <- function(x) {
  vctrs::vec_check_size(x, 1, arg = rlang::caller_arg(x), call = rlang::caller_env())
  vctrs::vec_cast(x, numeric())
}

cast_scalar_integer <- function(x) {
  vctrs::vec_check_size(x, 1, arg = rlang::caller_arg(x), call = rlang::caller_env())
  vctrs::vec_cast(x, integer())
}

cast_nullable_string <- function(x) {
  if (is.null(x)) {
    return(NULL)
  }

  cast_string(x)
}

cast_nullable_scalar_double <- function(x) {
  if (is.null(x)) {
    return(NULL)
  }

  cast_scalar_double(x)
}

cast_nullable_scalar_integer <- function(x) {
  if (is.null(x)) {
    return(NULL)
  }

  cast_scalar_integer(x)
}


cast_double <- function(x) {
  vctrs::vec_cast(x, numeric())
}

cast_integer <- function(x) {
  vctrs::vec_cast(x, integer())
}

cast_list <- function(x, ptype, allow_null = FALSE) {
  if (is.null(x)) {
    if (allow_null) {
      return(NULL)
    } else {
      rlang::abort("{.arg x} must not be `NULL`.")
    }
  }

  if (is.list(x)) {
    x <- vctrs::list_unchop(x)
  }
  x <- vctrs::vec_cast(x, to = ptype)
  vctrs::vec_chop(x)
}

cast_string_list <- function(x, allow_null = FALSE) {
  cast_list(x, character(), allow_null = allow_null)
}

cast_integer_list <- function(x, allow_null = FALSE) {
  cast_list(x, integer(), allow_null = allow_null)
}

cast_double_list <- function(x, allow_null = FALSE) {
  cast_list(x, numeric(), allow_null = allow_null)
}

cast_choice <- function(x, choices, error_arg = rlang::caller_arg(x), error_call = rlang::caller_env()) {
  rlang::arg_match(x, choices, error_arg = error_arg, error_call = error_call)
}
