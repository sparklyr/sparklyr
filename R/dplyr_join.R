#' Join Spark tbls.
#'
#' These functions are wrappers around their `dplyr` equivalents that set
#' Spark SQL-compliant values for the `suffix` argument by replacing dots (`.`)
#' with underscores (`_`). See [join] for a description of the general purpose
#' of the functions.
#'
#' @inheritParams dbplyr::join.tbl_sql
#'
#' @name join.tbl_spark
NULL

#' @rdname join.tbl_spark
#' @export
#' @importFrom dplyr inner_join
inner_join.tbl_spark <-
  function(x,
           y,
           by = NULL,
           copy = FALSE,
           suffix = c("_x", "_y"),
           auto_index = FALSE,
           ...,
           sql_on = NULL) {
    if (any(grepl("\\.", suffix))) {
      suffix <- gsub("\\.", "_", suffix)
      message(
        "Replacing '.' with '_' in suffixes. New suffixes: ",
        paste(suffix, collapse = ", ")
      )
    }

    NextMethod(suffix = suffix)
  }

#' @rdname join.tbl_spark
#' @export
#' @importFrom dplyr left_join
left_join.tbl_spark <-
  function(x,
           y,
           by = NULL,
           copy = FALSE,
           suffix = c("_x", "_y"),
           auto_index = FALSE,
           ...,
           sql_on = NULL) {
    if (any(grepl("\\.", suffix))) {
      suffix <- gsub("\\.", "_", suffix)
      message(
        "Replacing '.' with '_' in suffixes. New suffixes: ",
        paste(suffix, collapse = ", ")
      )
    }

    NextMethod(suffix = suffix)
  }

#' @rdname join.tbl_spark
#' @export
#' @importFrom dplyr right_join
right_join.tbl_spark <-
  function(x,
           y,
           by = NULL,
           copy = FALSE,
           suffix = c("_x", "_y"),
           auto_index = FALSE,
           ...,
           sql_on = NULL) {
    if (any(grepl("\\.", suffix))) {
      suffix <- gsub("\\.", "_", suffix)
      message(
        "Replacing '.' with '_' in suffixes. New suffixes: ",
        paste(suffix, collapse = ", ")
      )
    }

    NextMethod(suffix = suffix)
  }

#' @rdname join.tbl_spark
#' @export
#' @importFrom dplyr full_join
full_join.tbl_spark <-
  function(x,
           y,
           by = NULL,
           copy = FALSE,
           suffix = c("_x", "_y"),
           auto_index = FALSE,
           ...,
           sql_on = NULL) {
    if (any(grepl("\\.", suffix))) {
      suffix <- gsub("\\.", "_", suffix)
      message(
        "Replacing '.' with '_' in suffixes. New suffixes: ",
        paste(suffix, collapse = ", ")
      )
    }

    NextMethod(suffix = suffix)
  }
