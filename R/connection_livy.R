livy_get_sessions <- function(master) {
  fromJSON(paste(master, "sessions", sep = "/"))
}

livy_validate_master <- function(master) {
  tryCatch({
    livy_get_sessions(master)
  }, error = function(err) {
    stop("Failed to connect to Livy service at ", master, ". ", err$message)
  })

  NULL
}

#' @import jsonlite
livy_connection <- function(master) {
  livy_validate_master(master)

  list(
    master = master
  )
}
