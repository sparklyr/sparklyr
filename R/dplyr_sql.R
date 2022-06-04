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
#' @importFrom dbplyr sql_build
sql_build.op_sample_n <- function(op, con, ...) {
  if (rlang::quo_is_null(op$args$weight)) {
    sql_build.op_sample(op, con, frac = FALSE)
  } else {
    sql_build.op_weighted_sample(op, con, frac = FALSE)
  }
}

#' @export
#' @importFrom dbplyr sql_build
sql_build.op_sample_frac <- function(op, con, ...) {
  if (rlang::quo_is_null(op$args$weight)) {
    sql_build.op_sample(op, con, frac = TRUE)
  } else {
    sql_build.op_weighted_sample(op, con, frac = TRUE)
  }
}

sql_build.op_sample <- function(op, con, frac) {
  grps <- dbplyr::op_grps(op)
  sdf <- to_sdf(op, con)
  cols <- colnames(sdf)

  sample_sdf <- (
    if (length(grps) > 0) {
      if (frac) {
        sdf_stratified_sample_frac(
          x = sdf,
          grps = grps,
          frac = op$args$size,
          weight = NULL,
          replace = op$args$replace,
          op$args$seed
        )
      } else {
        sdf_stratified_sample_n(
          x = sdf,
          grps = grps,
          k = op$args$size,
          weight = NULL,
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
        weight_col = NULL,
        k = sample_size,
        replacement = op$args$replace,
        seed = op$args$seed
      )
    })

  sample_sdf %>% dbplyr::remote_query()
}

#' @export
sql_build.lazy_sample_query <- function(op, con, ...) {
  grps <- dbplyr::op_grps(op$x)
  sdf <- to_sdf(op$x, con)
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

  sample_sdf %>% dbplyr::remote_query()
}

sql_build.op_weighted_sample <- function(op, con, frac) {
  grps <- dbplyr::op_grps(op)
  sdf <- to_sdf(op, con)

  weight <- rlang::as_name(op$args$weight)

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

  sample_sdf %>% dbplyr::remote_query()
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

    row_num <- random_string("__row_num")
    args <- list(
      .keep_all = .keep_all,
      .row_num = row_num,
      .all_cols = all_cols,
      .distinct_cols = distinct_cols
    )

    if (dbplyr_uses_ops()) {
      add_op_single("tbl_spark_distinct", .data, args = args)
    } else {
      row_num_sql <- list(dplyr::sql("ROW_NUMBER() OVER (ORDER BY NULL)"))
      names(row_num_sql) <- row_num
      .data <- .data %>% dplyr::mutate(!!!row_num_sql)

      if (.keep_all) {
        out_cols <- all_cols
      } else {
        out_cols <- distinct_cols
      }

      exprs <- lapply(
        purrr::set_names(c(row_num, out_cols)),
        function(x) rlang::expr(FIRST(!!sym(x), FALSE))
      )

      grps <- dplyr::group_vars(.data)
      out <- .data %>%
        dplyr::group_by(!!!syms(distinct_cols)) %>%
        summarise(!!!exprs) %>%
        select(all_of(out_cols)) %>%
        arrange(!!sym(row_num)) %>%
        dplyr::group_by(!!!syms(grps))
      out$order_vars <- NULL
      out
    }
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

#' @export
#' @importFrom dbplyr op_vars
#' @importFrom dbplyr sql_build
#' @importFrom dbplyr select_query
#' @importFrom purrr map_chr
sql_build.op_tbl_spark_distinct <- function(op, con, ...) {
  if(op$args$.keep_all) {
    dc <- op$args$.distinct_cols
    al <- op$args$.all_cols
    alr <- remove_matching_strings(dc, al)
    quoted_names <- map_chr(alr, quote_sql_name)
    first_select <- sql(paste0("first(", quoted_names, ") as ", quoted_names, collapse = ", "))
    group_names <- sql_collapse(dc)
    full_select <- sql(paste(group_names, first_select, sep = ","))
    select_query(
      select = full_select,
      from = sql_build(op$x, con = con),
      group_by = group_names,
      distinct = FALSE
    )
  } else {
    select_query(
      from = sql_build(op$x, con = con),
      select = sql_collapse(op_vars(op)),
      distinct = TRUE
    )
  }

}

#' @export
#' @importFrom dbplyr op_vars
op_vars.op_tbl_spark_distinct <- function(op) {
  if (op$args$.keep_all) {
    op$args$.all_cols
  } else {
    op$args$.distinct_cols
  }
}

to_sdf <- function(op, con) {
  sdf_sql(
    con,
    dbplyr::select_query(
      from = dbplyr::sql(
        dbplyr::sql_render(dbplyr::sql_build(op$x, con = con), con = con),
        con = con
      ),
      select = dbplyr::build_sql("*", con = con)
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

