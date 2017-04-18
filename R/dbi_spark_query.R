select_spark_query <- function(from,
                         select = sql("*"),
                         where = character(),
                         group_by = character(),
                         having = character(),
                         order_by = character(),
                         distinct = FALSE,
                         limit = numeric()) {

  stopifnot(is.character(select))
  stopifnot(is.character(where))
  stopifnot(is.character(group_by))
  stopifnot(is.character(having))
  stopifnot(is.character(order_by))
  stopifnot(is.logical(distinct), length(distinct) == 1L)
  stopifnot(is.numeric(limit) || is.null(limit))

  structure(
    list(
      from = from,
      select = select,
      where = where,
      group_by = group_by,
      having = having,
      order_by = order_by,
      distinct = distinct,
      limit = limit
    ),
    class = c("select_spark_query", "select_query", "query")
  )
}

sql_render.select_spark_query <- function(query, con = NULL, ..., root = FALSE) {
  from <- sql_subquery(con, sql_render(query$from, con, ..., root = root), name = NULL)

  sql_select(
    con, query$select, from, where = query$where, group_by = query$group_by,
    having = query$having, order_by = query$order_by, distinct = query$distinct,
    limit = query$limit, ...
  )
}

#' @export
print.select_spark_query <- function(x, ...) {
  cat("<SQL SELECT", if (x$distinct) " DISTINCT", ">\n", sep = "")
  cat("From:     ", x$from, "\n", sep = "")

  if (length(x$select))   cat("Select:   ", named_commas(x$select), "\n", sep = "")
  if (length(x$where))    cat("Where:    ", named_commas(x$where), "\n", sep = "")
  if (length(x$group_by)) cat("Group by: ", named_commas(x$group_by), "\n", sep = "")
  if (length(x$order_by)) cat("Order by: ", named_commas(x$order_by), "\n", sep = "")
  if (length(x$having))   cat("Having:   ", named_commas(x$having), "\n", sep = "")
  if (length(x$limit))    cat("Limit:    ", named_commas(x$limit), "\n", sep = "")
}
