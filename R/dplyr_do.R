#' @export
#' @importFrom dplyr do
#' @importFrom dplyr groups
#' @importFrom dplyr select
#' @importFrom dplyr count
do.tbl_spark <- function(.data, ...) {
  sdf <- spark_dataframe(.data)
  quosures <- rlang::quos(...)

  # get column references to grouped variables
  groups <- as.character(as.list(groups(.data)))
  columns <- lapply(groups, function(group) {
    invoke(sdf, "col", group)
  })

  # compute unique combinations of values
  combos <- sdf %>%
    invoke("select", columns) %>%
    invoke("groupBy", columns) %>%
    invoke("count") %>%
    sdf_collect() %>%
    select(-count)

  # apply function on subsets of data
  outputs <- vector("list", nrow(combos))
  nm <- names(combos)

  lapply(seq_len(nrow(combos)), function(i) {

    # generate filters for each combination
    filters <- lapply(seq_along(nm), function(j) {
      sdf %>%
        invoke("col", nm[[j]]) %>%
        invoke("equalTo", combos[[j]][[i]])
    })

    # apply filters
    filtered <- sdf
    for (filter in filters) {
      filtered <- spark_dataframe(filtered) %>%
        invoke("filter", filter) %>%
        sdf_register()
    }


    # apply functions with this data
    fits <- enumerate(quosures, function(name, quosure) {

      # override '.' in envir
      assign(".", filtered, envir = environment(quosure))

      # evaluate in environment
      tryCatch(
        rlang::eval_tidy(quosure),
        error = identity
      )
    })

    # store
    outputs[[i]] <<- fits
  })

  # produce 'result' dataset by adding outputs to 'combos'
  result <- combos
  columns <- lapply(names(quosures), function(name) {
    lapply(outputs, `[[`, name)
  })

  for (i in seq_along(quosures)) {
    key <- names(quosures)[[i]]
    val <- lapply(outputs, `[[`, key)
    result[[key]] <- val
  }

  result
}
