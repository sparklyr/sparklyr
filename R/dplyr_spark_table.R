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
sample_n.tbl_spark <- function(tbl,
                               size,
                               replace = FALSE,
                               weight = NULL,
                               .env = parent.frame(),
                               ...) {
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

  tbl$lazy_query <- lazy_sample_query(tbl$lazy_query, frac = FALSE, args = args)

  tbl %>%
    as_sampled_tbl(frac = FALSE, args = args)
}

#' @export
#' @importFrom dplyr sample_frac
sample_frac.tbl_spark <- function(tbl,
                                  size = 1,
                                  replace = FALSE,
                                  weight = NULL,
                                  .env = parent.frame(),
                                  ...) {
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

  tbl$lazy_query <- lazy_sample_query(tbl$lazy_query, frac = TRUE, args = args)

  tbl %>%
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
slice_.tbl_spark <- function(.data, ..., .dots) {
  stop("Slice is not supported in this version of sparklyr")
}

#' @export
#' @importFrom dplyr tbl_ptype
tbl_ptype.tbl_spark <- function(.data) {
  simulate_vars_spark(.data)
}

gen_prng_seed <- function() {
  if (is.null(get0(".Random.seed"))) {
    NULL
  } else {
    as.integer(sample.int(.Machine$integer.max, size = 1L))
  }
}
