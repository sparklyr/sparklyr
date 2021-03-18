#' @include sdf_interface.R
#' @include sdf_sql.R
#' @include stratified_sample.R
#' @include utils.R
NULL

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
    row_num_sql <- list(dplyr::sql("ROW_NUMBER() OVER (ORDER BY NULL)"))
    names(row_num_sql) <- row_num
    .data <- .data %>>% dplyr::mutate %@% row_num_sql
    args <- list(
      .keep_all = .keep_all,
      .row_num = row_num,
      .all_cols = all_cols,
      .distinct_cols = distinct_cols
    )

    add_op_single("tbl_spark_distinct", .data, args = args)
  }
}

#' @export
#' @importFrom dbplyr op_vars
#' @importFrom dbplyr sql_build
sql_build.op_tbl_spark_distinct <- function(op, con, ...) {
  output_cols <- op_vars(op)
  sql <- lapply(
    c(op$args$.row_num, output_cols),
    function(x) {
      x <- quote_sql_name(x, con)
      sprintf("FIRST(%s, FALSE) AS %s", x, x)
    }
  ) %>%
    paste(collapse = ", ") %>%
    dbplyr::sql()

  dbplyr::select_query(
    from = dbplyr::sql_build(op$x, con = con),
    select = sql,
    group_by = op$args$.distinct_cols %>%
      lapply(function(x) quote_sql_name(x, con)) %>%
      paste(collapse = ", ") %>%
      dbplyr::sql(),
    order_by = quote_sql_name(op$args$.row_num, con) %>% dbplyr::sql()
  ) %>%
    dbplyr::select_query(
      from = .,
      select = output_cols %>%
        lapply(function(x) quote_sql_name(x, con)) %>%
        paste(collapse = ", ") %>%
        dbplyr::sql(),
      order_by = quote_sql_name(op$args$.row_num, con) %>% dbplyr::sql()
    )
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
