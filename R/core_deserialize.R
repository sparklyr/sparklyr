# nolint start
# Type mapping from Java to R
#
# void -> NULL
# Int -> integer
# String -> character
# Boolean -> logical
# Float -> double
# Double -> double
# Long -> double
# Array[Byte] -> raw
# Date -> Date
# Time -> POSIXct
#
# Array[T] -> list()
# Object -> jobj
#
# nolint end

readObject <- function(con) {
  # Read type first
  type <- readType(con)
  readTypedObject(con, type)
}

readTypedObject <- function(con, type) {
  switch (type,
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
          "n" = NULL,
          "j" = getJobj(con, readString(con)),
          stop(paste("Unsupported type for deserialization", type)))
}

readString <- function(con) {
  stringLen <- readInt(con)
  raw <- readBin(con, raw(), stringLen, endian = "big")
  string <- rawToChar(raw)
  Encoding(string) <- "UTF-8"
  string
}

readInt <- function(con, n = 1) {
  readBin(con, integer(), n = n, endian = "big")
}

readDouble <- function(con, n = 1) {
  readBin(con, double(), n = n, endian = "big")
}

readBoolean <- function(con, n = 1) {
  as.logical(readInt(con, n = n))
}

readType <- function(con) {
  rawToChar(readBin(con, "raw", n = 1L))
}

readDate <- function(con) {
  as.Date(readString(con))
}

readTime <- function(con, n = 1) {
  t <- readDouble(con, n)
  as.POSIXct(t, origin = "1970-01-01")
}

readArray <- function(con) {
  type <- readType(con)
  len <- readInt(con)

  # short-circuit for reading arrays of double, int, logical
  if (type == "d") {
    return(readDouble(con, n = len))
  } else if (type == "i") {
    return(readInt(con, n = len))
  } else if (type == "b") {
    return(readBoolean(con, n = len))
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
  readBin(con, raw(), as.integer(dataLen), endian = "big")
}

readRawLen <- function(con, dataLen) {
  readBin(con, raw(), as.integer(dataLen), endian = "big")
}

readDeserialize <- function(con) {
  # We have two cases that are possible - In one, the entire partition is
  # encoded as a byte array, so we have only one value to read. If so just
  # return firstData
  dataLen <- readInt(con)
  firstData <- unserialize(
    readBin(con, raw(), as.integer(dataLen), endian = "big"))

  # Else, read things into a list
  dataLen <- readInt(con)
  if (length(dataLen) > 0 && dataLen > 0) {
    data <- list(firstData)
    while (length(dataLen) > 0 && dataLen > 0) {
      data[[length(data) + 1L]] <- unserialize(
        readBin(con, raw(), as.integer(dataLen), endian = "big"))
      dataLen <- readInt(con)
    }
    unlist(data, recursive = FALSE)
  } else {
    firstData
  }
}

readMultipleObjects <- function(inputCon) {
  # readMultipleObjects will read multiple continuous objects from
  # a DataOutputStream. There is no preceding field telling the count
  # of the objects, so the number of objects varies, we try to read
  # all objects in a loop until the end of the stream.
  data <- list()
  while (TRUE) {
    # If reaching the end of the stream, type returned should be "".
    type <- readType(inputCon)
    if (type == "") {
      break
    }
    data[[length(data) + 1L]] <- readTypedObject(inputCon, type)
  }
  data # this is a list of named lists now
}

readRowList <- function(obj) {
  # readRowList is meant for use inside an lapply. As a result, it is
  # necessary to open a standalone connection for the row and consume
  # the numCols bytes inside the read function in order to correctly
  # deserialize the row.
  rawObj <- rawConnection(obj, "r+")
  on.exit(close(rawObj))
  readObject(rawObj)
}
