read_bin <- function(con, what, n, endian = NULL) {
  UseMethod("read_bin")
}

read_bin.default <- function(con, what, n, endian = NULL) {
  if (is.null(endian)) readBin(con, what, n) else readBin(con, what, n, endian = endian)
}

read_bin_wait <- function(con, what, n, endian = NULL) {
  sc <- con
  con <- if (!is.null(sc$state) && identical(sc$state$use_monitoring, TRUE)) sc$monitoring else sc$backend

  timeout <- spark_config_value(sc$config, "sparklyr.backend.timeout", 30 * 24 * 60 * 60)
  progressInterval <- spark_config_value(sc$config, "sparklyr.progress.interval", 3)

  result <- if (is.null(endian)) readBin(con, what, n) else readBin(con, what, n, endian = endian)

  progressTimeout <- Sys.time() + progressInterval
  if (is.null(sc$state$progress)) {
    sc$state$progress <- new.env()
  }
  progressUpdated <- FALSE

  waitInterval <- 0
  commandStart <- Sys.time()
  while (length(result) == 0 && commandStart + timeout > Sys.time()) {
    Sys.sleep(waitInterval)
    waitInterval <- min(0.1, waitInterval + 0.01)

    result <- if (is.null(endian)) readBin(con, what, n) else readBin(con, what, n, endian = endian)

    if (Sys.time() > progressTimeout) {
      progressTimeout <- Sys.time() + progressInterval
      if (exists("connection_progress")) {
        connection_progress(sc)
        progressUpdated <- TRUE
      }
    }
  }

  if (progressUpdated) connection_progress_terminated(sc)

  if (commandStart + timeout <= Sys.time()) {
    stop("Operation timed out, increase config option sparklyr.backend.timeout if needed.")
  }

  result
}

read_bin.spark_connection <- function(con, what, n, endian = NULL) {
  read_bin_wait(con, what, n, endian)
}

read_bin.spark_worker_connection <- function(con, what, n, endian = NULL) {
  read_bin_wait(con, what, n, endian)
}

read_bin.livy_backend <- function(con, what, n, endian = NULL) {
  read_bin.default(con$rc, what, n, endian)
}

readObject <- function(con) {
  # Read type first
  type <- readType(con)
  readTypedObject(con, type)
}

readTypedObject <- function(con, type) {
  switch(type,
    "i" = readInt(con),
    "c" = readString(con),
    "b" = readBoolean(con),
    "d" = readDouble(con),
    "r" = readRaw(con),
    "D" = readDate(con),
    "t" = readTime(con),
    "a" = readArray(con),
    "l" = readList(con),
    "e" = readEnv(con),
    "s" = readStruct(con),
    "f" = readFastStringArray(con),
    "n" = NULL,
    "j" = getJobj(con, readString(con)),
    "J" = jsonlite::fromJSON(
      readString(con), simplifyDataFrame = FALSE, simplifyMatrix = FALSE
    ),
    stop(paste("Unsupported type for deserialization", type))
  )
}

readString <- function(con) {
  stringLen <- readInt(con)
  string <- ""

  if (stringLen > 0) {
    raw <- read_bin(con, raw(), stringLen, endian = "big")
    if (is.element("00", raw)) {
      warning("Input contains embedded nuls, removing.")
      raw <- raw[raw != "00"]
    }
    string <- rawToChar(raw)
  }

  Encoding(string) <- "UTF-8"
  string
}

readFastStringArray <- function(con) {
  joined <- readString(con)
  as.list(strsplit(joined, "\u0019")[[1]])
}

readDateArray <- function(con, n = 1) {
  if (n == 0) {
    as.Date(NA)
  } else {
    do.call(c, lapply(seq(n), function(x) readDate(con)))
  }
}

readInt <- function(con, n = 1) {
  if (n == 0) {
    integer(0)
  } else {
    read_bin(con, integer(), n = n, endian = "big")
  }
}

readDouble <- function(con, n = 1) {
  if (n == 0) {
    double(0)
  } else {
    read_bin(con, double(), n = n, endian = "big")
  }
}

readBoolean <- function(con, n = 1) {
  if (n == 0) {
    logical(0)
  } else {
    as.logical(readInt(con, n = n))
  }
}

readType <- function(con) {
  rawToChar(read_bin(con, "raw", n = 1L))
}

readDate <- function(con) {
  date_str <- readString(con)
  if (is.null(date_str) || identical(date_str, "")) {
    as.Date(NA)
  } else if (getOption("sparklyr.collect.datechars", FALSE)) {
    date_str
  } else {
    as.Date(date_str, tz = "UTC")
  }
}

readTime <- function(con, n = 1) {
  if (identical(n, 0)) {
    as.POSIXct(character(0))
  } else {
    t <- readDouble(con, n)

    r <- as.POSIXct(t, origin = "1970-01-01", tz = "UTC")
    if (getOption("sparklyr.collect.datechars", FALSE)) {
      as.character(r)
    } else {
      r
    }
  }
}

readArray <- function(con) {
  type <- readType(con)
  len <- readInt(con)

  if (type == "d") {
    return(readDouble(con, n = len))
  } else if (type == "i") {
    return(readInt(con, n = len))
  } else if (type == "b") {
    return(readBoolean(con, n = len))
  } else if (type == "t") {
    return(readTime(con, n = len))
  } else if (type == "D") {
    return(readDateArray(con, n = len))
  }

  if (len > 0) {
    l <- vector("list", len)
    for (i in 1:len) {
      l[[i]] <- readTypedObject(con, type)
    }
    l
  } else {
    list()
  }
}

# Read a list. Types of each element may be different.
# Null objects are read as NA.
readList <- function(con) {
  len <- readInt(con)
  if (len > 0) {
    l <- vector("list", len)
    for (i in 1:len) {
      elem <- readObject(con)
      if (is.null(elem)) {
        elem <- NA
      }
      l[[i]] <- elem
    }
    l
  } else {
    list()
  }
}

readEnv <- function(con) {
  env <- new.env()
  len <- readInt(con)
  if (len > 0) {
    for (i in 1:len) {
      key <- readString(con)
      value <- readObject(con)
      env[[key]] <- value
    }
  }
  env
}

# Convert a named list to struct so that
# SerDe won't confuse between a normal named list and struct
listToStruct <- function(list) {
  stopifnot(class(list) == "list")
  stopifnot(!is.null(names(list)))
  class(list) <- "struct"
  list
}

# Read a field of StructType from DataFrame
# into a named list in R whose class is "struct"
readStruct <- function(con) {
  names <- readObject(con)
  fields <- readObject(con)
  names(fields) <- names
  listToStruct(fields)
}

readRaw <- function(con) {
  dataLen <- readInt(con)
  if (dataLen == 0) {
    raw()
  } else {
    read_bin(con, raw(), as.integer(dataLen), endian = "big")
  }
}
