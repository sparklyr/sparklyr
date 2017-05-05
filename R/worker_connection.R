connection_is_open <- function(sc) {
  bothOpen <- FALSE
  if (!identical(sc, NULL)) {
    tryCatch({
      bothOpen <- isOpen(sc$backend) && isOpen(sc$monitor)
    }, error = function(e) {
    })
  }
  bothOpen
}
