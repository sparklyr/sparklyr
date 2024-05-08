#' @include sdf_interface.R
#' @include sdf_sql.R
#' @include stratified_sample.R
#' @include utils.R
NULL

lazy_sample_query <- function(x, frac, args) {
  f_lazy_query <- utils::getFromNamespace("lazy_query", "dbplyr")

  f_lazy_query(
    query_type = "sample",
    x = x,
    args = args,
    frac = frac
  )
}

#' @export
op_vars.lazy_sample_query <- function(op) {
  op_vars(op$x)
}

#' @export
sql_build.lazy_sample_query <- function(op, con, ...) {
  grps <- dbplyr::op_grps(op$x)
  sdf <- to_sdf(op, con)
  frac <- op$frac

  if (rlang::quo_is_null(op$args$weight)) {
    weight <- NULL
  } else {
    weight <- rlang::as_name(op$args$weight)
  }

  sample_sdf <- (
    if (length(grps) > 0) {
      if (frac) {
        sdf_stratified_sample_frac(
          x = sdf,
          grps = grps,
          frac = op$args$size,
          weight = weight,
          replace = op$args$replace,
          op$args$seed
        )
      } else {
        sdf_stratified_sample_n(
          x = sdf,
          grps = grps,
          k = op$args$size,
          weight = weight,
          replace = op$args$replace,
          op$args$seed
        )
      }
    } else {
      sample_size <- (
        if (frac) {
          cnt <- sdf %>%
            spark_dataframe() %>%
            invoke("count")
          round(cnt * check_frac(op$args$size, replace = op$args$replace))
        } else {
          op$args$size
        })
      sdf_weighted_sample(
        x = sdf,
        weight_col = weight,
        k = sample_size,
        replacement = op$args$replace,
        seed = op$args$seed
      )
    })

  dbplyr::sql_build(sample_sdf)
}

#' @export
#' @importFrom dplyr distinct
distinct.tbl_spark <- function(.data, ..., .keep_all = FALSE) {
  if (identical(getOption("sparklyr.dplyr_distinct.impl"), "tbl_lazy")) {
    NextMethod()
  } else {
    if (rlang::dots_n(...) > 0) {
      dots <- rlang::enexprs(...)
      .data <- .data %>% dplyr::mutate(...)
      distinct_cols <- lapply(
        seq_along(dots),
        function(i) {
          x <- dots[i]
          if (identical(names(x), "")) {
            rlang::as_name(dots[[i]])
          } else {
            names(x)
          }
        }
      ) %>%
        unlist()
    } else {
      distinct_cols <- colnames(.data)
    }
    distinct_cols <- union(dplyr::group_vars(.data), distinct_cols)
    all_cols <- colnames(.data)

    if (.keep_all) {
      out_cols <- all_cols
    } else {
      out_cols <- distinct_cols
    }

    exprs <- lapply(
      purrr::set_names(out_cols),
      function(x) rlang::expr(FIRST(!!sym(x), FALSE))
    )

    grps <- dplyr::group_vars(.data)
    out <- .data %>%
      dplyr::group_by(!!!syms(distinct_cols)) %>%
      summarise(!!!exprs) %>%
      select(all_of(out_cols)) %>%
      dplyr::group_by(!!!syms(grps))
    out$order_vars <- NULL
    out

  }
}
utils::globalVariables(c("FIRST"))

sql_collapse <- function(x) {
  sql(paste0(map_chr(x, quote_sql_name), collapse = ", "))
}

remove_matching_strings <- function(x, y) {
  for(i in seq_along(x)) {
    y <- y[y != x[i]]
  }
  y
}

to_sdf <- function(op, con) {
  sdf_sql(
    con,
    dbplyr::select_query(
      from = dbplyr::sql_build(op$x, con = con),
      select = dbplyr::sql("*")
    ) %>%
      dbplyr::sql_render(con = con) %>%
      dbplyr::sql() %>%
      as.character() %>%
      paste0(collapse = "")
  )
}

check_frac <- function(size, replace = FALSE) {
  if (size <= 1 || replace) {
    return(invisible(size))
  }

  rlang::abort(
    "size", "of sampled fraction must be less or equal to one, ",
    "set `replace` = TRUE to use sampling with replacement"
  )
}

#' @export
head.tbl_spark <- function(x, n = 6L, ...) {
  if (ncol(x) == 0) {
    return(x)
  } else {
    NextMethod()
  }
}
