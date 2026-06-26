# Utility functions to serialize R objects so they can be read in Java.

# nolint start
# Type mapping from R to Java
#
# NULL -> Void
# integer -> Int
# character -> String
# logical -> Boolean
# double, numeric -> Double
# raw -> Array[Byte]
# Date -> Date
# POSIXct,POSIXlt -> Timestamp
#
# list[T] -> Array[T], where T is one of above mentioned types
# environment -> Map[String, T], where T is a native type
# jobj -> Object, where jobj is an object created in the backend
# nolint end

getSerdeType <- function(object) {
  type <- class(object)[[1]]

  if (type != "list") {
    type
  } else {
    # Check if all elements are of same type
    elemType <- unique(sapply(object, function(elem) {
      getSerdeType(elem)
    }))
    if (length(elemType) <= 1) {
      # Check that there are no NAs in arrays since they are unsupported in scala
      hasNAs <- any(is.na(object))

      if (hasNAs) {
        "list"
      } else {
        "array"
      }
    } else {
      "list"
    }
  }
}

writeObject <- function(con, object, writeType = TRUE) {
  type <- class(object)[[1]]

  if (
    type %in%
      c(
        "integer",
        "character",
        "logical",
        "double",
        "numeric",
        "factor",
        "Date",
        "POSIXct"
      )
  ) {
    if (is.na(object) && !is.nan(object)) {
      object <- NULL
      type <- "NULL"
    }
  }

  serdeType <- getSerdeType(object)
  if (writeType) {
    writeType(con, serdeType)
  }
  switch(
    serdeType,
    NULL = writeVoid(con),
    integer = writeInt(con, object),
    character = writeString(con, object),
    logical = writeBoolean(con, object),
    double = writeDouble(con, object),
    numeric = writeDouble(con, object),
    raw = writeRaw(con, object),
    array = writeArray(con, object),
    list = writeList(con, object),
    struct = writeList(con, object),
    spark_jobj = writeJobj(con, object),
    environment = writeEnv(con, object),
    Date = writeDate(con, object),
    POSIXlt = writeTime(con, object),
    POSIXct = writeTime(con, object),
    factor = writeFactor(con, object),
    `data.frame` = writeList(con, object),
    spark_apply_binary_result = writeList(con, object),
    stop("Unsupported type '", serdeType, "' for serialization")
  )
}

writeVoid <- function(con) {
  # no value for NULL
}

writeJobj <- function(con, value) {
  if (!isValidJobj(value)) {
    stop("invalid jobj ", value$id)
  }
  writeString(con, value$id)
}

writeString <- function(con, value) {
  utfVal <- enc2utf8(as.character(value))
  writeInt(con, as.integer(nchar(utfVal, type = "bytes") + 1))
  writeBin(utfVal, con, endian = "big", useBytes = TRUE)
}

writeInt <- function(con, value) {
  writeBin(as.integer(value), con, endian = "big")
}

writeDouble <- function(con, value) {
  writeBin(value, con, endian = "big")
}

writeBoolean <- function(con, value) {
  # TRUE becomes 1, FALSE becomes 0
  writeInt(con, as.integer(value))
}

writeRaw <- function(con, batch) {
  writeInt(con, length(batch))
  writeBin(batch, con, endian = "big")
}

writeType <- function(con, class) {
  type <- switch(
    class,
    NULL = "n",
    integer = "i",
    character = "c",
    logical = "b",
    double = "d",
    numeric = "d",
    raw = "r",
    array = "a",
    list = "l",
    struct = "s",
    spark_jobj = "j",
    environment = "e",
    Date = "D",
    POSIXlt = "t",
    POSIXct = "t",
    factor = "c",
    `data.frame` = "l",
    spark_apply_binary_result = "l",
    stop("Unsupported type '", class, "' for serialization")
  )
  writeBin(charToRaw(type), con)
}

# Used to pass arrays where all the elements are of the same type
writeArray <- function(con, arr) {
  # TODO: Empty lists are given type "character" right now.
  # This may not work if the Java side expects array of any other type.
  if (length(arr) == 0) {
    elemType <- class("somestring")
  } else {
    elemType <- getSerdeType(arr[[1]])
  }

  writeType(con, elemType)
  writeInt(con, length(arr))

  if (length(arr) > 0) {
    for (a in arr) {
      writeObject(con, a, FALSE)
    }
  }
}

# Used to pass arrays where the elements can be of different types
writeList <- function(con, list) {
  writeInt(con, length(list))
  for (elem in list) {
    writeObject(con, elem)
  }
}

# Used to pass in hash maps required on Java side.
writeEnv <- function(con, env) {
  len <- length(env)

  writeInt(con, len)
  if (len > 0) {
    writeArray(con, as.list(ls(env)))
    vals <- lapply(ls(env), function(x) {
      env[[x]]
    })
    writeList(con, as.list(vals))
  }
}

writeDate <- function(con, date) {
  writeInt(con, as.integer(date))
}

writeTime <- function(con, time) {
  writeDouble(con, as.double(time))
}

writeFactor <- function(con, factor) {
  writeString(con, as.character(factor))
}

# Used to serialize in a list of objects where each
# object can be of a different type. Serialization format is
# <object type> <object> for each object
writeArgs <- function(con, args) {
  if (length(args) > 0) {
    for (a in args) {
      writeObject(con, a)
    }
  }
}

read_bin <- function(con, what, n, endian = NULL) {
  UseMethod("read_bin")
}

#' @export
read_bin.default <- function(con, what, n, endian = NULL) {
  if (is.null(endian)) {
    readBin(con, what, n)
  } else {
    readBin(con, what, n, endian = endian)
  }
}

read_bin_wait <- function(con, what, n, endian = NULL) {
  sc <- con
  con <- if (!is.null(sc$state) && identical(sc$state$use_monitoring, TRUE)) {
    sc$monitoring
  } else {
    sc$backend
  }

  timeout <- spark_config_value(
    sc$config,
    "sparklyr.backend.timeout",
    30 * 24 * 60 * 60
  )
  progressInterval <- spark_config_value(
    sc$config,
    "sparklyr.progress.interval",
    3
  )

  result <- if (is.null(endian)) {
    readBin(con, what, n)
  } else {
    readBin(con, what, n, endian = endian)
  }

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

    result <- if (is.null(endian)) {
      readBin(con, what, n)
    } else {
      readBin(con, what, n, endian = endian)
    }

    if (Sys.time() > progressTimeout) {
      progressTimeout <- Sys.time() + progressInterval
      if (exists("connection_progress")) {
        connection_progress(sc)
        progressUpdated <- TRUE
      }
    }
  }

  if (progressUpdated) {
    connection_progress_terminated(sc)
  }

  if (commandStart + timeout <= Sys.time()) {
    stop(
      "Operation timed out, increase config option sparklyr.backend.timeout if needed."
    )
  }

  result
}

read_bin.spark_connection <- function(con, what, n, endian = NULL) {
  read_bin_wait(con, what, n, endian)
}

#' @export
read_bin.spark_worker_connection <- function(con, what, n, endian = NULL) {
  read_bin_wait(con, what, n, endian)
}

#' @export
read_bin.livy_backend <- function(con, what, n, endian = NULL) {
  read_bin.default(con$rc, what, n, endian)
}

readObject <- function(con) {
  # Read type first
  type <- readType(con)
  readTypedObject(con, type)
}

readTypedObject <- function(con, type) {
  switch(
    type,
    "i" = readInt(con),
    "c" = readString(con),
    "b" = readBoolean(con),
    "d" = readDouble(con),
    "r" = readRaw(con),
    "D" = readDate(con),
    "t" = readTime(con),
    "a" = readArray(con),
    "l" = readList(con),
    "e" = readMap(con),
    "s" = readStruct(con),
    "f" = readStringArray(con),
    "n" = NULL,
    "j" = getJobj(con, readString(con)),
    "J" = jsonlite::fromJSON(
      readString(con),
      simplifyDataFrame = FALSE,
      simplifyMatrix = FALSE
    ),
    stop(paste("Unsupported type for deserialization", type))
  )
}

readString <- function(con) {
  stringLen <- readInt(con)

  string <- (if (stringLen > 0) {
    raw <- read_bin(con, raw(), stringLen, endian = "big")
    if (is.element("00", raw)) {
      warning("Input contains embedded nuls, removing.")
      raw <- raw[raw != "00"]
    }
    rawToChar(raw)
  } else if (stringLen == 0) {
    ""
  } else {
    NA_character_
  })

  Encoding(string) <- "UTF-8"
  string
}

readStringArray <- function(con) {
  joined <- readString(con)
  arr <- as.list(strsplit(joined, "\u0019")[[1]])
  lapply(
    arr,
    function(x) {
      if (x == "<NA>") NA_character_ else x
    }
  )
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
  n <- readInt(con)
  if (is.na(n)) {
    as.Date(NA)
  } else {
    d <- as.Date(n, origin = "1970-01-01")
    if (getOption("sparklyr.collect.datechars", FALSE)) {
      as.character(d)
    } else {
      d
    }
  }
}

readTime <- function(con, n = 1) {
  if (identical(n, 0)) {
    as.POSIXct(character(0))
  } else {
    t <- readDouble(con, n)

    r <- as.POSIXct(t, origin = "1970-01-01")
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
    for (i in seq_len(len)) {
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
    for (i in seq_len(len)) {
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

readMap <- function(con) {
  map <- list()
  len <- readInt(con)
  if (len > 0) {
    for (i in seq_len(len)) {
      key <- readString(con)
      value <- readObject(con)
      map[[key]] <- value
    }
  }

  map
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
  if (dataLen == -1) {
    NA
  } else if (dataLen == 0) {
    raw()
  } else {
    read_bin(con, raw(), as.integer(dataLen), endian = "big")
  }
}
