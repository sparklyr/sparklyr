# This file contains helper methods that are useful for working with tidyr.

# emit an error if the given arg is missing
check_present <- function(x) {
  arg <- rlang::ensym(x)
  if (missing(x)) {
    abort(paste0("Argument `", arg, "` is missing with no default"))
  }
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

# helper method for updating dplyr group variables
update_group_vars <- function(input, output, preserved) {
  incl <- dplyr::group_vars(input)
  output <- do.call(dplyr::group_by, append(list(output), lapply(incl, as.symbol)))

  excl <- setdiff(incl, preserved)
  if (length(excl) > 0) {
    output <- do.call(dplyr::ungroup, append(list(output), lapply(excl, as.symbol)))
  }

  output
}

strip_names <- function(df, base, names_sep) {
  base <- paste0(base, names_sep)
  names <- names(df)

  has_prefix <- regexpr(base, names, fixed = TRUE) == 1L
  names[has_prefix] <- substr(names[has_prefix], nchar(base) + 1, nchar(names[has_prefix]))

  rlang::set_names(df, names)
}

# Given a list of column names possibly containing duplicates and a valid tibble
# name-repair strategy, apply that strategy to the column names and return the
# result. For compatibility with Spark SQL, all '.'s in column names will be
# replaced with '_'.
repair_names <- function(col_names, names_repair) {
  args <- as.list(rep(NA, length(col_names)))
  names(args) <- col_names
  args <- append(args, list(.name_repair = names_repair))

  do.call(tibble::tibble, args) %>%
    names() %>%
    lapply(function(x) gsub("\\.", "_", x)) %>%
    unlist()
}

# If x is already a Spark data frame, then return dbplyr::remote_name(x)
# Otherwise ensure the result from Spark SQL query encapsulated by x is
# materialized into a Spark temp view and return the name of that temp view
ensure_tmp_view <- function(x) {
  dbplyr::remote_name(x) %||% {
    sc <- spark_connection(x)
    sdf <- spark_dataframe(x)
    data_tmp_view_name <- sparklyr:::random_string("sparklyr_tmp_")
    if (spark_version(sc) < "2.0.0") {
      invoke(sdf, "registerTempTable", data_tmp_view_name)
    } else {
      invoke(sdf, "createOrReplaceTempView", data_tmp_view_name)
    }

    data_tmp_view_name
  }
}
