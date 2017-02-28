#' @export
mutate_.tbl_spark <- function(.data, ..., .dots) {
  dots <- lazyeval::all_dots(.dots, ..., all_named = TRUE)
  dots <- partial_eval(dots, vars = op_vars(.data))

  if (packageVersion("dplyr") > "0.5.0")
    dots <- partial_eval(dots, op_vars(.data))

  data <- .data
  lapply(seq_along(dots), function(i) {
    data <<- dplyr::add_op_single("mutate", data, dots = dots[i])
  })

  data
}
