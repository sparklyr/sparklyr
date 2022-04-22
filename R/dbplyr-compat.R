add_op_single <- function(...) {
  if (dbplyr_uses_ops()) {
    f <- utils::getFromNamespace("distinct.tbl_lazy", "dbplyr")
    f(...)
  }
}
