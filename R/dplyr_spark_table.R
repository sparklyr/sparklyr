#' @include ml_feature_sql_transformer_utils.R
#' @include prng_utils.R
NULL

#' @export
#' @importFrom dplyr collect
collect.spark_jobj <- function(x, ...) {
  sdf_collect(x, ...)
}

#' @export
#' @importFrom dplyr collect
collect.tbl_spark <- function(x, ...) {
  sdf_collect(x, ...)
}

#' @export
#' @importFrom dplyr sample_n
#' @importFrom dbplyr add_op_single
sample_n.tbl_spark <- function(tbl,
                               size,
                               replace = FALSE,
                               weight = NULL,
                               .env = parent.frame()) {
  if (spark_version(spark_connection(tbl)) < "2.0.0") {
    stop("sample_n() is not supported until Spark 2.0 or later. Use sdf_sample instead.")
  }

  args <- list(
    size = size,
    replace = replace,
    weight = rlang::enquo(weight),
    seed = gen_prng_seed(),
    .env = .env
  )

  add_op_single("sample_n", .data = tbl, args = args) %>%
    as_sampled_tbl(frac = FALSE, args = args)
}

#' @export
#' @importFrom dplyr sample_frac
#' @importFrom dbplyr add_op_single
sample_frac.tbl_spark <- function(tbl,
                                  size = 1,
                                  replace = FALSE,
                                  weight = NULL,
                                  .env = parent.frame()) {
  if (spark_version(spark_connection(tbl)) < "2.0.0") {
    stop("sample_frac() is not supported until Spark 2.0 or later.")
  }

  args <- list(
    size = size,
    replace = replace,
    weight = rlang::enquo(weight),
    seed = gen_prng_seed(),
    .env = .env
  )
  add_op_single("sample_frac", .data = tbl, args = args) %>%
    as_sampled_tbl(frac = TRUE, args = args)
}

as_sampled_tbl <- function(tbl, frac, args) {
  attributes(tbl)$sampling_params <- structure(list(
    frac = frac,
    args = args,
    group_by = dbplyr::op_grps(tbl)
  ))

  tbl
}

#' @export
#' @importFrom dplyr slice_
slice_.tbl_spark <- function(x, ...) {
  stop("Slice is not supported in this version of sparklyr")
}

#' @export
head.tbl_spark_print <- function(x, ...) {
  head(as.data.frame(x), ...)
}

#' @export
dim.tbl_spark_print <- function(x) {
  attributes(x)$spark_dims
}

#' @importFrom tibble tbl_sum
#' @export
tbl_sum.tbl_spark_print <- function(x) {
  attributes(x)$spark_summary
}

#' @export
print.tbl_spark <- function(x, ...) {
  sdf <- spark_dataframe(x)

  if (exists(".rs.S3Overrides")) {
    # if RStudio is overriding the print functions do not attempt to custom print tibble
    if (exists("print.tbl_sql", envir = get(".rs.S3Overrides"))) {
      if (sdf_is_streaming(sdf)) {
        rows <- getOption("max.print", 1000)
        data <- sdf_collect(sdf, n = rows)
      } else {
        data <- x
        class(data) <- class(data)[-match("tbl_spark", class(data))]
      }

      print(data)
      return(invisible(x))
    }
  }

  rows <- getOption("tibble.print_min", getOption("dplyr.print_min", 10))
  options <- list(...)
  if ("n" %in% names(options)) {
    rows <- max(rows, options$n)
  }

  grps <- dbplyr::op_grps(x$ops)
  sort <- dbplyr::op_sort(x$ops) %>%
    purrr::map_if(rlang::is_formula, rlang::f_rhs) %>%
    purrr::map_chr(rlang::expr_text, width = 500L)

  mark <- if (identical(getOption("OutDec"), ",")) "." else ","
  cols_fmt <- formatC(dim(x)[2], big.mark = mark)

  # collect rows + 1 to ensure that tibble knows there is more data to collect
  if (sdf_is_streaming(sdf)) {
    rows_fmt <- "inf"
    data <- sdf_collect(sdf, n = rows + 1)
  } else {
    rows_fmt <- "??"
    data <- dplyr::collect(head(x, n = rows + 1))
  }

  attributes(data)$spark_dims <- c(NA_real_, sdf_ncol(x))

  remote_name <- sdf_remote_name(x)
  remote_name <- if (is.null(remote_name) || grepl("^sparklyr_tmp_", remote_name)) "?" else remote_name

  attributes(data)$spark_summary <- c(
    Source = paste0(
      "spark<",
      remote_name,
      "> [", rows_fmt, " x ", cols_fmt, "]"
    ),
    if (length(grps) > 0) c(Groups = paste0(grps, collapse = ", ")),
    if (length(sort) > 0) c(`Ordered by` = paste0(sort, collapse = ", "))
  )

  data <- tibble::as_tibble(as.data.frame(data))
  class(data) <- c("tbl_spark_print", class(data))

  print(data, ...)
}

#' @export
#' @importFrom dplyr tbl_ptype
tbl_ptype.tbl_spark <- function(.data) {
  simulate_vars(.data)
}
