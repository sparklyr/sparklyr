#' @include ml_feature_sql_transformer.R
NULL

new_ml_sample_transformer <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_sample_transformer")
}

#' @rdname sql-transformer
#'
#' @details \code{ft_dplyr_transformer()} is mostly a wrapper around \code{ft_sql_transformer()} that
#'   takes a \code{tbl_spark} instead of a SQL statement. Internally, the \code{ft_dplyr_transformer()}
#'   extracts the \code{dplyr} transformations used to generate \code{tbl} as a SQL statement or a
#'   sampling operation. Note that only single-table \code{dplyr} verbs are supported and that the
#'   \code{sdf_} family of functions are not.
#'
#' @param tbl A \code{tbl_spark} generated using \code{dplyr} transformations.
#' @export
ft_dplyr_transformer <- function(x, tbl,
                                 uid = random_string("dplyr_transformer_"), ...) {
  UseMethod("ft_dplyr_transformer")
}

ml_dplyr_transformer <- ft_dplyr_transformer

#' @export
ft_dplyr_transformer.spark_connection <- function(x, tbl,
                                                  uid = random_string("dplyr_transformer_"), ...) {
  if (!identical(class(tbl)[1], "tbl_spark")) stop("'tbl' must be a Spark table")

  if (is.null(attributes(tbl)$sampling_params)) {
    ft_sql_transformer(x, ft_extract_sql(tbl), uid = uid)
  } else {
    sc <- spark_connection(tbl)

    sampling_params <- attributes(tbl)$sampling_params
    if (sampling_params$frac) {
      jobj <- invoke_new(sc, "sparklyr.SampleFrac", uid) %>%
        invoke("setFrac", sampling_params$args$size)
    } else {
      jobj <- invoke_new(sc, "sparklyr.SampleN", uid) %>%
        invoke("setN", as.integer(sampling_params$args$size))
    }
    jobj <- jobj %>%
      invoke(
        "%>%",
        list(
          "setWeight",
          if (rlang::quo_is_null(sampling_params$args$weight)) {
            ""
          } else {
            rlang::as_name(sampling_params$args$weight)
          }
        ),
        list("setReplace", sampling_params$args$replace),
        list("setGroupBy", as.list(sampling_params$group_by)),
        list("setSeed", as.integer(sampling_params$args$seed %||% Sys.time()))
      )

    new_ml_sample_transformer(jobj)
  }
}

#' @export
ft_dplyr_transformer.ml_pipeline <- function(x, tbl,
                                             uid = random_string("dplyr_transformer_"), ...) {
  stage <- ft_dplyr_transformer.spark_connection(
    x = spark_connection(x),
    tbl = tbl,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_dplyr_transformer.tbl_spark <- function(x, tbl,
                                           uid = random_string("dplyr_transformer_"), ...) {
  stage <- ft_dplyr_transformer.spark_connection(
    x = spark_connection(x),
    tbl = tbl,
    uid = uid,
    ...
  )
  ml_transform(stage, x)
}

ft_extract_sql <- function(x) {

  get_base_name <- function(o) {
    if (!inherits(o$x, "lazy_base_query")) {
      get_base_name(o$x)
    } else {
      o$x$x
    }
  }

  tbl_name <- get_base_name(x$lazy_query)
  if (packageVersion("dbplyr") > "2.3.4") {
    tbl_name <- format(tbl_name)
    tbl_name <- substr(tbl_name, 2, nchar(tbl_name) - 1)
  }
  pattern <- paste0("\\b", tbl_name, "\\b")

  gsub(pattern, "__THIS__", dbplyr::sql_render(x))
}
