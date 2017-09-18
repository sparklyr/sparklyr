package sparklyr

object Sources {
  def sources: String = "" +
    "#' A helper function to retrieve values from \\code{spark_config()}\n" +
    "#'\n" +
    "#' @param config The configuration list from \\code{spark_config()}\n" +
    "#' @param name The name of the configuration entry\n" +
    "#' @param default The default value to use when entry is not present\n" +
    "#'\n" +
    "#' @keywords internal\n" +
    "#' @export\n" +
    "spark_config_value <- function(config, name, default = NULL) {\n" +
    "  if (!name %in% names(config)) default else config[[name]]\n" +
    "}\n" +
    "#' Check whether the connection is open\n" +
    "#'\n" +
    "#' @param sc \\code{spark_connection}\n" +
    "#'\n" +
    "#' @keywords internal\n" +
    "#'\n" +
    "#' @export\n" +
    "connection_is_open <- function(sc) {\n" +
    "  UseMethod(\"connection_is_open\")\n" +
    "}\n" +
    "# nolint start\n" +
    "# Type mapping from Java to R\n" +
    "#\n" +
    "# void -> NULL\n" +
    "# Int -> integer\n" +
    "# String -> character\n" +
    "# Boolean -> logical\n" +
    "# Float -> double\n" +
    "# Double -> double\n" +
    "# Long -> double\n" +
    "# Array[Byte] -> raw\n" +
    "# Date -> Date\n" +
    "# Time -> POSIXct\n" +
    "#\n" +
    "# Array[T] -> list()\n" +
    "# Object -> jobj\n" +
    "#\n" +
    "# nolint end\n" +
    "\n" +
    "readObject <- function(con) {\n" +
    "  # Read type first\n" +
    "  type <- readType(con)\n" +
    "  readTypedObject(con, type)\n" +
    "}\n" +
    "\n" +
    "readTypedObject <- function(con, type) {\n" +
    "  switch (type,\n" +
    "          \"i\" = readInt(con),\n" +
    "          \"c\" = readString(con),\n" +
    "          \"b\" = readBoolean(con),\n" +
    "          \"d\" = readDouble(con),\n" +
    "          \"r\" = readRaw(con),\n" +
    "          \"D\" = readDate(con),\n" +
    "          \"t\" = readTime(con),\n" +
    "          \"a\" = readArray(con),\n" +
    "          \"l\" = readList(con),\n" +
    "          \"e\" = readEnv(con),\n" +
    "          \"s\" = readStruct(con),\n" +
    "          \"n\" = NULL,\n" +
    "          \"j\" = getJobj(con, readString(con)),\n" +
    "          stop(paste(\"Unsupported type for deserialization\", type)))\n" +
    "}\n" +
    "\n" +
    "readString <- function(con) {\n" +
    "  stringLen <- readInt(con)\n" +
    "  raw <- readBin(con, raw(), stringLen, endian = \"big\")\n" +
    "  string <- rawToChar(raw)\n" +
    "  Encoding(string) <- \"UTF-8\"\n" +
    "  string\n" +
    "}\n" +
    "\n" +
    "readInt <- function(con, n = 1) {\n" +
    "  readBin(con, integer(), n = n, endian = \"big\")\n" +
    "}\n" +
    "\n" +
    "readDouble <- function(con, n = 1) {\n" +
    "  readBin(con, double(), n = n, endian = \"big\")\n" +
    "}\n" +
    "\n" +
    "readBoolean <- function(con, n = 1) {\n" +
    "  as.logical(readInt(con, n = n))\n" +
    "}\n" +
    "\n" +
    "readType <- function(con) {\n" +
    "  rawToChar(readBin(con, \"raw\", n = 1L))\n" +
    "}\n" +
    "\n" +
    "readDate <- function(con) {\n" +
    "  as.Date(readString(con))\n" +
    "}\n" +
    "\n" +
    "readTime <- function(con, n = 1) {\n" +
    "  t <- readDouble(con, n)\n" +
    "  as.POSIXct(t, origin = \"1970-01-01\")\n" +
    "}\n" +
    "\n" +
    "readArray <- function(con) {\n" +
    "  type <- readType(con)\n" +
    "  len <- readInt(con)\n" +
    "\n" +
    "  # short-circuit for reading arrays of double, int, logical\n" +
    "  if (type == \"d\") {\n" +
    "    return(readDouble(con, n = len))\n" +
    "  } else if (type == \"i\") {\n" +
    "    return(readInt(con, n = len))\n" +
    "  } else if (type == \"b\") {\n" +
    "    return(readBoolean(con, n = len))\n" +
    "  }\n" +
    "\n" +
    "  if (len > 0) {\n" +
    "    l <- vector(\"list\", len)\n" +
    "    for (i in 1:len) {\n" +
    "      l[[i]] <- readTypedObject(con, type)\n" +
    "    }\n" +
    "    l\n" +
    "  } else {\n" +
    "    list()\n" +
    "  }\n" +
    "}\n" +
    "\n" +
    "# Read a list. Types of each element may be different.\n" +
    "# Null objects are read as NA.\n" +
    "readList <- function(con) {\n" +
    "  len <- readInt(con)\n" +
    "  if (len > 0) {\n" +
    "    l <- vector(\"list\", len)\n" +
    "    for (i in 1:len) {\n" +
    "      elem <- readObject(con)\n" +
    "      if (is.null(elem)) {\n" +
    "        elem <- NA\n" +
    "      }\n" +
    "      l[[i]] <- elem\n" +
    "    }\n" +
    "    l\n" +
    "  } else {\n" +
    "    list()\n" +
    "  }\n" +
    "}\n" +
    "\n" +
    "readEnv <- function(con) {\n" +
    "  env <- new.env()\n" +
    "  len <- readInt(con)\n" +
    "  if (len > 0) {\n" +
    "    for (i in 1:len) {\n" +
    "      key <- readString(con)\n" +
    "      value <- readObject(con)\n" +
    "      env[[key]] <- value\n" +
    "    }\n" +
    "  }\n" +
    "  env\n" +
    "}\n" +
    "\n" +
    "# Convert a named list to struct so that\n" +
    "# SerDe won't confuse between a normal named list and struct\n" +
    "listToStruct <- function(list) {\n" +
    "  stopifnot(class(list) == \"list\")\n" +
    "  stopifnot(!is.null(names(list)))\n" +
    "  class(list) <- \"struct\"\n" +
    "  list\n" +
    "}\n" +
    "\n" +
    "# Read a field of StructType from DataFrame\n" +
    "# into a named list in R whose class is \"struct\"\n" +
    "readStruct <- function(con) {\n" +
    "  names <- readObject(con)\n" +
    "  fields <- readObject(con)\n" +
    "  names(fields) <- names\n" +
    "  listToStruct(fields)\n" +
    "}\n" +
    "\n" +
    "readRaw <- function(con) {\n" +
    "  dataLen <- readInt(con)\n" +
    "  readBin(con, raw(), as.integer(dataLen), endian = \"big\")\n" +
    "}\n" +
    "\n" +
    "readRawLen <- function(con, dataLen) {\n" +
    "  readBin(con, raw(), as.integer(dataLen), endian = \"big\")\n" +
    "}\n" +
    "\n" +
    "readDeserialize <- function(con) {\n" +
    "  # We have two cases that are possible - In one, the entire partition is\n" +
    "  # encoded as a byte array, so we have only one value to read. If so just\n" +
    "  # return firstData\n" +
    "  dataLen <- readInt(con)\n" +
    "  firstData <- unserialize(\n" +
    "    readBin(con, raw(), as.integer(dataLen), endian = \"big\"))\n" +
    "\n" +
    "  # Else, read things into a list\n" +
    "  dataLen <- readInt(con)\n" +
    "  if (length(dataLen) > 0 && dataLen > 0) {\n" +
    "    data <- list(firstData)\n" +
    "    while (length(dataLen) > 0 && dataLen > 0) {\n" +
    "      data[[length(data) + 1L]] <- unserialize(\n" +
    "        readBin(con, raw(), as.integer(dataLen), endian = \"big\"))\n" +
    "      dataLen <- readInt(con)\n" +
    "    }\n" +
    "    unlist(data, recursive = FALSE)\n" +
    "  } else {\n" +
    "    firstData\n" +
    "  }\n" +
    "}\n" +
    "\n" +
    "readMultipleObjects <- function(inputCon) {\n" +
    "  # readMultipleObjects will read multiple continuous objects from\n" +
    "  # a DataOutputStream. There is no preceding field telling the count\n" +
    "  # of the objects, so the number of objects varies, we try to read\n" +
    "  # all objects in a loop until the end of the stream.\n" +
    "  data <- list()\n" +
    "  while (TRUE) {\n" +
    "    # If reaching the end of the stream, type returned should be \"\".\n" +
    "    type <- readType(inputCon)\n" +
    "    if (type == \"\") {\n" +
    "      break\n" +
    "    }\n" +
    "    data[[length(data) + 1L]] <- readTypedObject(inputCon, type)\n" +
    "  }\n" +
    "  data # this is a list of named lists now\n" +
    "}\n" +
    "\n" +
    "readRowList <- function(obj) {\n" +
    "  # readRowList is meant for use inside an lapply. As a result, it is\n" +
    "  # necessary to open a standalone connection for the row and consume\n" +
    "  # the numCols bytes inside the read function in order to correctly\n" +
    "  # deserialize the row.\n" +
    "  rawObj <- rawConnection(obj, \"r+\")\n" +
    "  on.exit(close(rawObj))\n" +
    "  readObject(rawObj)\n" +
    "}\n" +
    "wait_connect_gateway <- function(gatewayAddress, gatewayPort, config, isStarting) {\n" +
    "  waitSeconds <- if (isStarting)\n" +
    "    spark_config_value(config, \"sparklyr.gateway.start.timeout\", 60)\n" +
    "  else\n" +
    "    spark_config_value(config, \"sparklyr.gateway.connect.timeout\", 1)\n" +
    "\n" +
    "  gateway <- NULL\n" +
    "  commandStart <- Sys.time()\n" +
    "\n" +
    "  while (is.null(gateway) && Sys.time() < commandStart + waitSeconds) {\n" +
    "    tryCatch({\n" +
    "      suppressWarnings({\n" +
    "        timeout <- spark_config_value(config, \"sparklyr.monitor.timeout\", 1)\n" +
    "        gateway <- socketConnection(host = gatewayAddress,\n" +
    "                                    port = gatewayPort,\n" +
    "                                    server = FALSE,\n" +
    "                                    blocking = TRUE,\n" +
    "                                    open = \"rb\",\n" +
    "                                    timeout = timeout)\n" +
    "      })\n" +
    "    }, error = function(err) {\n" +
    "    })\n" +
    "\n" +
    "    startWait <- spark_config_value(config, \"sparklyr.gateway.start.wait\", 50 / 1000)\n" +
    "    Sys.sleep(startWait)\n" +
    "  }\n" +
    "\n" +
    "  gateway\n" +
    "}\n" +
    "\n" +
    "spark_gateway_commands <- function() {\n" +
    "  list(\n" +
    "    \"GetPorts\" = 0,\n" +
    "    \"RegisterInstance\" = 1\n" +
    "  )\n" +
    "}\n" +
    "\n" +
    "query_gateway_for_port <- function(gateway, sessionId, config, isStarting) {\n" +
    "  waitSeconds <- if (isStarting)\n" +
    "    spark_config_value(config, \"sparklyr.gateway.start.timeout\", 60)\n" +
    "  else\n" +
    "    spark_config_value(config, \"sparklyr.gateway.connect.timeout\", 1)\n" +
    "\n" +
    "  writeInt(gateway, spark_gateway_commands()[[\"GetPorts\"]])\n" +
    "  writeInt(gateway, sessionId)\n" +
    "  writeInt(gateway, if (isStarting) waitSeconds else 0)\n" +
    "\n" +
    "  backendSessionId <- NULL\n" +
    "  redirectGatewayPort <- NULL\n" +
    "\n" +
    "  commandStart <- Sys.time()\n" +
    "  while(length(backendSessionId) == 0 && commandStart + waitSeconds > Sys.time()) {\n" +
    "    backendSessionId <- readInt(gateway)\n" +
    "    Sys.sleep(0.1)\n" +
    "  }\n" +
    "\n" +
    "  redirectGatewayPort <- readInt(gateway)\n" +
    "  backendPort <- readInt(gateway)\n" +
    "\n" +
    "  if (length(backendSessionId) == 0 || length(redirectGatewayPort) == 0 || length(backendPort) == 0) {\n" +
    "    if (isStarting)\n" +
    "      stop(\"Sparklyr gateway did not respond while retrieving ports information after \", waitSeconds, \" seconds\")\n" +
    "    else\n" +
    "      return(NULL)\n" +
    "  }\n" +
    "\n" +
    "  list(\n" +
    "    gateway = gateway,\n" +
    "    backendPort = backendPort,\n" +
    "    redirectGatewayPort = redirectGatewayPort\n" +
    "  )\n" +
    "}\n" +
    "\n" +
    "spark_connect_gateway <- function(\n" +
    "  gatewayAddress,\n" +
    "  gatewayPort,\n" +
    "  sessionId,\n" +
    "  config,\n" +
    "  isStarting = FALSE) {\n" +
    "\n" +
    "  # try connecting to existing gateway\n" +
    "  gateway <- wait_connect_gateway(gatewayAddress, gatewayPort, config, isStarting)\n" +
    "\n" +
    "  if (is.null(gateway)) {\n" +
    "    if (isStarting)\n" +
    "      stop(\n" +
    "        \"Gateway in port (\", gatewayPort, \") did not respond.\")\n" +
    "\n" +
    "    NULL\n" +
    "  }\n" +
    "  else {\n" +
    "    worker_log(\"is querying ports from backend using port \", gatewayPort)\n" +
    "\n" +
    "    gatewayPortsQuery <- query_gateway_for_port(gateway, sessionId, config, isStarting)\n" +
    "    if (is.null(gatewayPortsQuery) && !isStarting) {\n" +
    "      close(gateway)\n" +
    "      return(NULL)\n" +
    "    }\n" +
    "\n" +
    "    redirectGatewayPort <- gatewayPortsQuery$redirectGatewayPort\n" +
    "    backendPort <- gatewayPortsQuery$backendPort\n" +
    "\n" +
    "    worker_log(\"found redirect gateway port \", redirectGatewayPort)\n" +
    "\n" +
    "    if (redirectGatewayPort == 0) {\n" +
    "      close(gateway)\n" +
    "\n" +
    "      if (isStarting)\n" +
    "        stop(\"Gateway in port (\", gatewayPort, \") does not have the requested session registered\")\n" +
    "\n" +
    "      NULL\n" +
    "    } else if(redirectGatewayPort != gatewayPort) {\n" +
    "      close(gateway)\n" +
    "\n" +
    "      spark_connect_gateway(gatewayAddress, redirectGatewayPort, sessionId, config, isStarting)\n" +
    "    }\n" +
    "    else {\n" +
    "      list(\n" +
    "        gateway = gateway,\n" +
    "        backendPort = backendPort\n" +
    "      )\n" +
    "    }\n" +
    "  }\n" +
    "}\n" +
    "core_invoke_method <- function(sc, static, object, method, ...)\n" +
    "{\n" +
    "  if (is.null(sc)) {\n" +
    "    stop(\"The connection is no longer valid.\")\n" +
    "  }\n" +
    "\n" +
    "  # if the object is a jobj then get it's id\n" +
    "  if (inherits(object, \"spark_jobj\"))\n" +
    "    object <- object$id\n" +
    "\n" +
    "  rc <- rawConnection(raw(), \"r+\")\n" +
    "  writeString(rc, object)\n" +
    "  writeBoolean(rc, static)\n" +
    "  writeString(rc, method)\n" +
    "\n" +
    "  args <- list(...)\n" +
    "  writeInt(rc, length(args))\n" +
    "  writeArgs(rc, args)\n" +
    "  bytes <- rawConnectionValue(rc)\n" +
    "  close(rc)\n" +
    "\n" +
    "  rc <- rawConnection(raw(0), \"r+\")\n" +
    "  writeInt(rc, length(bytes))\n" +
    "  writeBin(bytes, rc)\n" +
    "  con <- rawConnectionValue(rc)\n" +
    "  close(rc)\n" +
    "\n" +
    "  backend <- sc$backend\n" +
    "  writeBin(con, backend)\n" +
    "\n" +
    "  if (identical(object, \"Handler\") &&\n" +
    "      (identical(method, \"terminateBackend\") || identical(method, \"stopBackend\"))) {\n" +
    "    # by the time we read response, backend might be already down.\n" +
    "    return(NULL)\n" +
    "  }\n" +
    "\n" +
    "  returnStatus <- readInt(backend)\n" +
    "\n" +
    "  if (length(returnStatus) == 0) {\n" +
    "    # read the spark log\n" +
    "    msg <- core_read_spark_log_error(sc)\n" +
    "    close(sc$backend)\n" +
    "    close(sc$monitor)\n" +
    "    withr::with_options(list(\n" +
    "      warning.length = 8000\n" +
    "    ), {\n" +
    "      stop(\n" +
    "        \"Unexpected state in sparklyr backend, terminating connection: \",\n" +
    "        msg,\n" +
    "        call. = FALSE)\n" +
    "    })\n" +
    "  }\n" +
    "\n" +
    "  if (returnStatus != 0) {\n" +
    "    # get error message from backend and report to R\n" +
    "    msg <- readString(backend)\n" +
    "    withr::with_options(list(\n" +
    "      warning.length = 8000\n" +
    "    ), {\n" +
    "      if (nzchar(msg)) {\n" +
    "        core_handle_known_errors(sc, msg)\n" +
    "\n" +
    "        stop(msg, call. = FALSE)\n" +
    "      } else {\n" +
    "        # read the spark log\n" +
    "        msg <- core_read_spark_log_error(sc)\n" +
    "        stop(msg, call. = FALSE)\n" +
    "      }\n" +
    "    })\n" +
    "  }\n" +
    "\n" +
    "  class(backend) <- c(class(backend), \"shell_backend\")\n" +
    "\n" +
    "  object <- readObject(backend)\n" +
    "  attach_connection(object, sc)\n" +
    "}\n" +
    "\n" +
    "jobj_subclass.shell_backend <- function(con) {\n" +
    "  \"shell_jobj\"\n" +
    "}\n" +
    "\n" +
    "core_handle_known_errors <- function(sc, msg) {\n" +
    "  # Some systems might have an invalid hostname that Spark <= 2.0.1 fails to handle\n" +
    "  # gracefully and triggers unexpected errors such as #532. Under these versions,\n" +
    "  # we proactevely test getLocalHost() to warn users of this problem.\n" +
    "  if (grepl(\"ServiceConfigurationError.*tachyon\", msg, ignore.case = TRUE)) {\n" +
    "    warning(\n" +
    "      \"Failed to retrieve localhost, please validate that the hostname is correctly mapped. \",\n" +
    "      \"Consider running `hostname` and adding that entry to your `/etc/hosts` file.\"\n" +
    "    )\n" +
    "  }\n" +
    "  else if (grepl(\"check worker logs for details\", msg, ignore.case = TRUE) &&\n" +
    "           spark_master_is_local(sc$master)) {\n" +
    "    abort_shell(\n" +
    "      \"sparklyr worker rscript failure, check worker logs for details\",\n" +
    "      NULL, NULL, sc$output_file, sc$error_file)\n" +
    "  }\n" +
    "}\n" +
    "\n" +
    "core_read_spark_log_error <- function(sc) {\n" +
    "  # if there was no error message reported, then\n" +
    "  # return information from the Spark logs. return\n" +
    "  # all those with most recent timestamp\n" +
    "  msg <- \"failed to invoke spark command (unknown reason)\"\n" +
    "  try(silent = TRUE, {\n" +
    "    log <- readLines(sc$output_file)\n" +
    "    splat <- strsplit(log, \"\\\\s+\", perl = TRUE)\n" +
    "    n <- length(splat)\n" +
    "    timestamp <- splat[[n]][[2]]\n" +
    "    regex <- paste(\"\\\\b\", timestamp, \"\\\\b\", sep = \"\")\n" +
    "    entries <- grep(regex, log, perl = TRUE, value = TRUE)\n" +
    "    pasted <- paste(entries, collapse = \"\\n\")\n" +
    "    msg <- paste(\"failed to invoke spark command\", pasted, sep = \"\\n\")\n" +
    "  })\n" +
    "  msg\n" +
    "}\n" +
    "#' Retrieve a Spark JVM Object Reference\n" +
    "#'\n" +
    "#' This S3 generic is used for accessing the underlying Java Virtual Machine\n" +
    "#' (JVM) Spark objects associated with \\R objects. These objects act as\n" +
    "#' references to Spark objects living in the JVM. Methods on these objects\n" +
    "#' can be called with the \\code{\\link{invoke}} family of functions.\n" +
    "#'\n" +
    "#' @param x An \\R object containing, or wrapping, a \\code{spark_jobj}.\n" +
    "#' @param ... Optional arguments; currently unused.\n" +
    "#'\n" +
    "#' @seealso \\code{\\link{invoke}}, for calling methods on Java object references.\n" +
    "#'\n" +
    "#' @export\n" +
    "spark_jobj <- function(x, ...) {\n" +
    "  UseMethod(\"spark_jobj\")\n" +
    "}\n" +
    "\n" +
    "\n" +
    "#' @export\n" +
    "spark_jobj.default <- function(x, ...) {\n" +
    "  stop(\"Unable to retrieve a spark_jobj from object of class \",\n" +
    "       paste(class(x), collapse = \" \"), call. = FALSE)\n" +
    "}\n" +
    "\n" +
    "#' @export\n" +
    "spark_jobj.spark_jobj <- function(x, ...) {\n" +
    "  x\n" +
    "}\n" +
    "\n" +
    "#' @export\n" +
    "print.spark_jobj <- function(x, ...) {\n" +
    "  print_jobj(spark_connection(x), x, ...)\n" +
    "}\n" +
    "\n" +
    "#' Generic method for print jobj for a connection type\n" +
    "#'\n" +
    "#' @param sc \\code{spark_connection} (used for type dispatch)\n" +
    "#' @param jobj Object to print\n" +
    "#'\n" +
    "#' @keywords internal\n" +
    "#'\n" +
    "#' @export\n" +
    "print_jobj <- function(sc, jobj, ...) {\n" +
    "  UseMethod(\"print_jobj\")\n" +
    "}\n" +
    "\n" +
    "\n" +
    "# Maintain a reference count of Java object references\n" +
    "# This allows us to GC the java object when it is safe\n" +
    ".validJobjs <- new.env(parent = emptyenv())\n" +
    "\n" +
    "# List of object ids to be removed\n" +
    ".toRemoveJobjs <- new.env(parent = emptyenv())\n" +
    "\n" +
    "# Check if jobj was created with the current SparkContext\n" +
    "isValidJobj <- function(jobj) {\n" +
    "  TRUE\n" +
    "}\n" +
    "\n" +
    "getJobj <- function(con, objId) {\n" +
    "  newObj <- jobj_create(con, objId)\n" +
    "  if (exists(objId, .validJobjs)) {\n" +
    "    .validJobjs[[objId]] <- .validJobjs[[objId]] + 1\n" +
    "  } else {\n" +
    "    .validJobjs[[objId]] <- 1\n" +
    "  }\n" +
    "  newObj\n" +
    "}\n" +
    "\n" +
    "jobj_subclass <- function(con) {\n" +
    "  UseMethod(\"jobj_subclass\")\n" +
    "}\n" +
    "\n" +
    "# Handler for a java object that exists on the backend.\n" +
    "jobj_create <- function(con, objId) {\n" +
    "  if (!is.character(objId)) {\n" +
    "    stop(\"object id must be a character\")\n" +
    "  }\n" +
    "  # NOTE: We need a new env for a jobj as we can only register\n" +
    "  # finalizers for environments or external references pointers.\n" +
    "  obj <- structure(new.env(parent = emptyenv()), class = c(\"spark_jobj\", jobj_subclass(con)))\n" +
    "  obj$id <- objId\n" +
    "\n" +
    "  # Register a finalizer to remove the Java object when this reference\n" +
    "  # is garbage collected in R\n" +
    "  reg.finalizer(obj, cleanup.jobj)\n" +
    "  obj\n" +
    "}\n" +
    "\n" +
    "jobj_info <- function(jobj) {\n" +
    "  if (!inherits(jobj, \"spark_jobj\"))\n" +
    "    stop(\"'jobj_info' called on non-jobj\")\n" +
    "\n" +
    "  class <- NULL\n" +
    "  repr <- NULL\n" +
    "\n" +
    "  tryCatch({\n" +
    "    class <- invoke(jobj, \"getClass\")\n" +
    "    if (inherits(class, \"spark_jobj\"))\n" +
    "      class <- invoke(class, \"toString\")\n" +
    "  }, error = function(e) {\n" +
    "  })\n" +
    "  tryCatch({\n" +
    "    repr <- invoke(jobj, \"toString\")\n" +
    "  }, error = function(e) {\n" +
    "  })\n" +
    "  list(\n" +
    "    class = class,\n" +
    "    repr  = repr\n" +
    "  )\n" +
    "}\n" +
    "\n" +
    "jobj_inspect <- function(jobj) {\n" +
    "  print(jobj)\n" +
    "  if (!connection_is_open(spark_connection(jobj)))\n" +
    "    return(jobj)\n" +
    "\n" +
    "  class <- invoke(jobj, \"getClass\")\n" +
    "\n" +
    "  cat(\"Fields:\\n\")\n" +
    "  fields <- invoke(class, \"getDeclaredFields\")\n" +
    "  lapply(fields, function(field) { print(field) })\n" +
    "\n" +
    "  cat(\"Methods:\\n\")\n" +
    "  methods <- invoke(class, \"getDeclaredMethods\")\n" +
    "  lapply(methods, function(method) { print(method) })\n" +
    "\n" +
    "  jobj\n" +
    "}\n" +
    "\n" +
    "cleanup.jobj <- function(jobj) {\n" +
    "  if (isValidJobj(jobj)) {\n" +
    "    objId <- jobj$id\n" +
    "    # If we don't know anything about this jobj, ignore it\n" +
    "    if (exists(objId, envir = .validJobjs)) {\n" +
    "      .validJobjs[[objId]] <- .validJobjs[[objId]] - 1\n" +
    "\n" +
    "      if (.validJobjs[[objId]] == 0) {\n" +
    "        rm(list = objId, envir = .validJobjs)\n" +
    "        # NOTE: We cannot call removeJObject here as the finalizer may be run\n" +
    "        # in the middle of another RPC. Thus we queue up this object Id to be removed\n" +
    "        # and then run all the removeJObject when the next RPC is called.\n" +
    "        .toRemoveJobjs[[objId]] <- 1\n" +
    "      }\n" +
    "    }\n" +
    "  }\n" +
    "}\n" +
    "\n" +
    "clearJobjs <- function() {\n" +
    "  valid <- ls(.validJobjs)\n" +
    "  rm(list = valid, envir = .validJobjs)\n" +
    "\n" +
    "  removeList <- ls(.toRemoveJobjs)\n" +
    "  rm(list = removeList, envir = .toRemoveJobjs)\n" +
    "}\n" +
    "\n" +
    "attach_connection <- function(jobj, connection) {\n" +
    "\n" +
    "  if (inherits(jobj, \"spark_jobj\")) {\n" +
    "    jobj$connection <- connection\n" +
    "  }\n" +
    "  else if (is.list(jobj) || inherits(jobj, \"struct\")) {\n" +
    "    jobj <- lapply(jobj, function(e) {\n" +
    "      attach_connection(e, connection)\n" +
    "    })\n" +
    "  }\n" +
    "  else if (is.environment(jobj)) {\n" +
    "    jobj <- eapply(jobj, function(e) {\n" +
    "      attach_connection(e, connection)\n" +
    "    })\n" +
    "  }\n" +
    "\n" +
    "  jobj\n" +
    "}\n" +
    "# Utility functions to serialize R objects so they can be read in Java.\n" +
    "\n" +
    "# nolint start\n" +
    "# Type mapping from R to Java\n" +
    "#\n" +
    "# NULL -> Void\n" +
    "# integer -> Int\n" +
    "# character -> String\n" +
    "# logical -> Boolean\n" +
    "# double, numeric -> Double\n" +
    "# raw -> Array[Byte]\n" +
    "# Date -> Date\n" +
    "# POSIXct,POSIXlt -> Time\n" +
    "#\n" +
    "# list[T] -> Array[T], where T is one of above mentioned types\n" +
    "# environment -> Map[String, T], where T is a native type\n" +
    "# jobj -> Object, where jobj is an object created in the backend\n" +
    "# nolint end\n" +
    "\n" +
    "getSerdeType <- function(object) {\n" +
    "  type <- class(object)[[1]]\n" +
    "\n" +
    "  if (type != \"list\") {\n" +
    "    type\n" +
    "  } else {\n" +
    "    # Check if all elements are of same type\n" +
    "    elemType <- unique(sapply(object, function(elem) { getSerdeType(elem) }))\n" +
    "    if (length(elemType) <= 1) {\n" +
    "\n" +
    "      # Check that there are no NAs in character arrays since they are unsupported in scala\n" +
    "      hasCharNAs <- any(sapply(object, function(elem) {\n" +
    "        (is.factor(elem) || is.character(elem) || is.integer(elem)) && is.na(elem)\n" +
    "      }))\n" +
    "\n" +
    "      if (hasCharNAs) {\n" +
    "        \"list\"\n" +
    "      } else {\n" +
    "        \"array\"\n" +
    "      }\n" +
    "    } else {\n" +
    "      \"list\"\n" +
    "    }\n" +
    "  }\n" +
    "}\n" +
    "\n" +
    "writeObject <- function(con, object, writeType = TRUE) {\n" +
    "  # NOTE: In R vectors have same type as objects. So we don't support\n" +
    "  # passing in vectors as arrays and instead require arrays to be passed\n" +
    "  # as lists.\n" +
    "  type <- class(object)[[1]]  # class of POSIXlt is c(\"POSIXlt\", \"POSIXt\")\n" +
    "  # Checking types is needed here, since 'is.na' only handles atomic vectors,\n" +
    "  # lists and pairlists\n" +
    "  if (type %in% c(\"integer\", \"character\", \"logical\", \"double\", \"numeric\", \"factor\")) {\n" +
    "    if (is.na(object)) {\n" +
    "      object <- NULL\n" +
    "      type <- \"NULL\"\n" +
    "    }\n" +
    "  }\n" +
    "\n" +
    "  serdeType <- getSerdeType(object)\n" +
    "  if (writeType) {\n" +
    "    writeType(con, serdeType)\n" +
    "  }\n" +
    "  switch(serdeType,\n" +
    "         NULL = writeVoid(con),\n" +
    "         integer = writeInt(con, object),\n" +
    "         character = writeString(con, object),\n" +
    "         logical = writeBoolean(con, object),\n" +
    "         double = writeDouble(con, object),\n" +
    "         numeric = writeDouble(con, object),\n" +
    "         raw = writeRaw(con, object),\n" +
    "         array = writeArray(con, object),\n" +
    "         list = writeList(con, object),\n" +
    "         struct = writeList(con, object),\n" +
    "         spark_jobj = writeJobj(con, object),\n" +
    "         environment = writeEnv(con, object),\n" +
    "         Date = writeDate(con, object),\n" +
    "         POSIXlt = writeTime(con, object),\n" +
    "         POSIXct = writeTime(con, object),\n" +
    "         factor = writeFactor(con, object),\n" +
    "         stop(paste(\"Unsupported type for serialization\", type)))\n" +
    "}\n" +
    "\n" +
    "writeVoid <- function(con) {\n" +
    "  # no value for NULL\n" +
    "}\n" +
    "\n" +
    "writeJobj <- function(con, value) {\n" +
    "  if (!isValidJobj(value)) {\n" +
    "    stop(\"invalid jobj \", value$id)\n" +
    "  }\n" +
    "  writeString(con, value$id)\n" +
    "}\n" +
    "\n" +
    "writeString <- function(con, value) {\n" +
    "  utfVal <- enc2utf8(value)\n" +
    "  writeInt(con, as.integer(nchar(utfVal, type = \"bytes\") + 1))\n" +
    "  writeBin(utfVal, con, endian = \"big\", useBytes = TRUE)\n" +
    "}\n" +
    "\n" +
    "writeInt <- function(con, value) {\n" +
    "  writeBin(as.integer(value), con, endian = \"big\")\n" +
    "}\n" +
    "\n" +
    "writeDouble <- function(con, value) {\n" +
    "  writeBin(value, con, endian = \"big\")\n" +
    "}\n" +
    "\n" +
    "writeBoolean <- function(con, value) {\n" +
    "  # TRUE becomes 1, FALSE becomes 0\n" +
    "  writeInt(con, as.integer(value))\n" +
    "}\n" +
    "\n" +
    "writeRawSerialize <- function(outputCon, batch) {\n" +
    "  outputSer <- serialize(batch, ascii = FALSE, connection = NULL)\n" +
    "  writeRaw(outputCon, outputSer)\n" +
    "}\n" +
    "\n" +
    "writeRowSerialize <- function(outputCon, rows) {\n" +
    "  invisible(lapply(rows, function(r) {\n" +
    "    bytes <- serializeRow(r)\n" +
    "    writeRaw(outputCon, bytes)\n" +
    "  }))\n" +
    "}\n" +
    "\n" +
    "serializeRow <- function(row) {\n" +
    "  rawObj <- rawConnection(raw(0), \"wb\")\n" +
    "  on.exit(close(rawObj))\n" +
    "  writeList(rawObj, row)\n" +
    "  rawConnectionValue(rawObj)\n" +
    "}\n" +
    "\n" +
    "writeRaw <- function(con, batch) {\n" +
    "  writeInt(con, length(batch))\n" +
    "  writeBin(batch, con, endian = \"big\")\n" +
    "}\n" +
    "\n" +
    "writeType <- function(con, class) {\n" +
    "  type <- switch(class,\n" +
    "                 NULL = \"n\",\n" +
    "                 integer = \"i\",\n" +
    "                 character = \"c\",\n" +
    "                 logical = \"b\",\n" +
    "                 double = \"d\",\n" +
    "                 numeric = \"d\",\n" +
    "                 raw = \"r\",\n" +
    "                 array = \"a\",\n" +
    "                 list = \"l\",\n" +
    "                 struct = \"s\",\n" +
    "                 spark_jobj = \"j\",\n" +
    "                 environment = \"e\",\n" +
    "                 Date = \"D\",\n" +
    "                 POSIXlt = \"t\",\n" +
    "                 POSIXct = \"t\",\n" +
    "                 factor = \"c\",\n" +
    "                 stop(paste(\"Unsupported type for serialization\", class)))\n" +
    "  writeBin(charToRaw(type), con)\n" +
    "}\n" +
    "\n" +
    "# Used to pass arrays where all the elements are of the same type\n" +
    "writeArray <- function(con, arr) {\n" +
    "  # TODO: Empty lists are given type \"character\" right now.\n" +
    "  # This may not work if the Java side expects array of any other type.\n" +
    "  if (length(arr) == 0) {\n" +
    "    elemType <- class(\"somestring\")\n" +
    "  } else {\n" +
    "    elemType <- getSerdeType(arr[[1]])\n" +
    "  }\n" +
    "\n" +
    "  writeType(con, elemType)\n" +
    "  writeInt(con, length(arr))\n" +
    "\n" +
    "  if (length(arr) > 0) {\n" +
    "    for (a in arr) {\n" +
    "      writeObject(con, a, FALSE)\n" +
    "    }\n" +
    "  }\n" +
    "}\n" +
    "\n" +
    "# Used to pass arrays where the elements can be of different types\n" +
    "writeList <- function(con, list) {\n" +
    "  writeInt(con, length(list))\n" +
    "  for (elem in list) {\n" +
    "    writeObject(con, elem)\n" +
    "  }\n" +
    "}\n" +
    "\n" +
    "# Used to pass in hash maps required on Java side.\n" +
    "writeEnv <- function(con, env) {\n" +
    "  len <- length(env)\n" +
    "\n" +
    "  writeInt(con, len)\n" +
    "  if (len > 0) {\n" +
    "    writeArray(con, as.list(ls(env)))\n" +
    "    vals <- lapply(ls(env), function(x) { env[[x]] })\n" +
    "    writeList(con, as.list(vals))\n" +
    "  }\n" +
    "}\n" +
    "\n" +
    "writeDate <- function(con, date) {\n" +
    "  writeString(con, as.character(date))\n" +
    "}\n" +
    "\n" +
    "writeTime <- function(con, time) {\n" +
    "  writeDouble(con, as.double(time))\n" +
    "}\n" +
    "\n" +
    "writeFactor <- function(con, factor) {\n" +
    "  writeString(con, as.character(factor))\n" +
    "}\n" +
    "\n" +
    "# Used to serialize in a list of objects where each\n" +
    "# object can be of a different type. Serialization format is\n" +
    "# <object type> <object> for each object\n" +
    "writeArgs <- function(con, args) {\n" +
    "  if (length(args) > 0) {\n" +
    "    for (a in args) {\n" +
    "      writeObject(con, a)\n" +
    "    }\n" +
    "  }\n" +
    "}\n" +
    "core_spark_apply_bundle_path <- function() {\n" +
    "  file.path(tempdir(), \"packages.tar\")\n" +
    "}\n" +
    "\n" +
    "#' Creates a bundle of dependencies required by \\code{spark_apply()}\n" +
    "#'\n" +
    "#' @keywords internal\n" +
    "#' @export\n" +
    "core_spark_apply_bundle <- function() {\n" +
    "  packagesTar <- core_spark_apply_bundle_path()\n" +
    "\n" +
    "  args <- c(\"-cf\", packagesTar)\n" +
    "  lapply(.libPaths(), function(e) {\n" +
    "    args <<- c(args, \"-C\", e)\n" +
    "    args <<- c(args, \".\")\n" +
    "  })\n" +
    "\n" +
    "  if (!file.exists(packagesTar)) {\n" +
    "    system2(\"tar\", args)\n" +
    "  }\n" +
    "\n" +
    "  packagesTar\n" +
    "}\n" +
    "\n" +
    "core_spark_apply_unbundle_path <- function() {\n" +
    "  file.path(\"sparklyr-bundle\")\n" +
    "}\n" +
    "core_get_package_function <- function(packageName, functionName) {\n" +
    "  if (packageName %in% rownames(installed.packages()) &&\n" +
    "      exists(functionName, envir = asNamespace(packageName)))\n" +
    "    get(functionName, envir = asNamespace(packageName))\n" +
    "  else\n" +
    "    NULL\n" +
    "}\n" +
    "worker_config_serialize <- function(config) {\n" +
    "  paste(\n" +
    "    if (isTRUE(config$debug)) \"TRUE\" else \"FALSE\",\n" +
    "    spark_config_value(config, \"sparklyr.worker.gateway.port\", \"8880\"),\n" +
    "    spark_config_value(config, \"sparklyr.worker.gateway.address\", \"localhost\"),\n" +
    "    sep = \";\"\n" +
    "  )\n" +
    "}\n" +
    "\n" +
    "worker_config_deserialize <- function(raw) {\n" +
    "  parts <- strsplit(raw, \";\")[[1]]\n" +
    "\n" +
    "  list(\n" +
    "    debug = as.logical(parts[[1]]),\n" +
    "    sparklyr.gateway.port = as.integer(parts[[2]]),\n" +
    "    sparklyr.gateway.address = parts[[3]]\n" +
    "  )\n" +
    "}\n" +
    "spark_worker_apply <- function(sc) {\n" +
    "  hostContextId <- worker_invoke_method(sc, FALSE, \"Handler\", \"getHostContext\")\n" +
    "  worker_log(\"retrieved worker context id \", hostContextId)\n" +
    "\n" +
    "  context <- structure(\n" +
    "    class = c(\"spark_jobj\", \"shell_jobj\"),\n" +
    "    list(\n" +
    "      id = hostContextId,\n" +
    "      connection = sc\n" +
    "    )\n" +
    "  )\n" +
    "\n" +
    "  worker_log(\"retrieved worker context\")\n" +
    "\n" +
    "  bundlePath <- worker_invoke(context, \"getBundlePath\")\n" +
    "  if (nchar(bundlePath) > 0) {\n" +
    "    worker_log(\"using bundle \", bundlePath)\n" +
    "\n" +
    "    bundleName <- basename(bundlePath)\n" +
    "\n" +
    "    workerRootDir <- worker_invoke_static(sc, \"org.apache.spark.SparkFiles\", \"getRootDirectory\")\n" +
    "    sparkBundlePath <- file.path(workerRootDir, bundleName)\n" +
    "\n" +
    "    if (!file.exists(sparkBundlePath)) {\n" +
    "      stop(\"failed to find bundle under SparkFiles root directory\")\n" +
    "    }\n" +
    "\n" +
    "    unbundlePath <- worker_spark_apply_unbundle(sparkBundlePath, workerRootDir)\n" +
    "\n" +
    "    .libPaths(unbundlePath)\n" +
    "    worker_log(\"updated .libPaths with bundle packages\")\n" +
    "  }\n" +
    "\n" +
    "  grouped_by <- worker_invoke(context, \"getGroupBy\")\n" +
    "  grouped <- !is.null(grouped_by) && length(grouped_by) > 0\n" +
    "  if (grouped) worker_log(\"working over grouped data\")\n" +
    "\n" +
    "  length <- worker_invoke(context, \"getSourceArrayLength\")\n" +
    "  worker_log(\"found \", length, \" rows\")\n" +
    "\n" +
    "  groups <- worker_invoke(context, if (grouped) \"getSourceArrayGroupedSeq\" else \"getSourceArraySeq\")\n" +
    "  worker_log(\"retrieved \", length(groups), \" rows\")\n" +
    "\n" +
    "  closureRaw <- worker_invoke(context, \"getClosure\")\n" +
    "  closure <- unserialize(closureRaw)\n" +
    "\n" +
    "  closureRLangRaw <- worker_invoke(context, \"getClosureRLang\")\n" +
    "  if (length(closureRLangRaw) > 0) {\n" +
    "    worker_log(\"found rlang closure\")\n" +
    "    closureRLang <- spark_worker_rlang_unserialize()\n" +
    "    if (!is.null(closureRLang)) {\n" +
    "      closure <- closureRLang(closureRLangRaw)\n" +
    "      worker_log(\"created rlang closure\")\n" +
    "    }\n" +
    "  }\n" +
    "\n" +
    "  columnNames <- worker_invoke(context, \"getColumns\")\n" +
    "\n" +
    "  if (!grouped) groups <- list(list(groups))\n" +
    "\n" +
    "  all_results <- NULL\n" +
    "\n" +
    "  for (group_entry in groups) {\n" +
    "    # serialized groups are wrapped over single lists\n" +
    "    data <- group_entry[[1]]\n" +
    "\n" +
    "    df <- do.call(rbind.data.frame, data)\n" +
    "    result <- NULL\n" +
    "\n" +
    "    if (nrow(df) == 0) {\n" +
    "      worker_log(\"found that source has no rows to be proceesed\")\n" +
    "    }\n" +
    "    else {\n" +
    "      colnames(df) <- columnNames[1: length(colnames(df))]\n" +
    "\n" +
    "      closure_params <- length(formals(closure))\n" +
    "      closure_args <- c(\n" +
    "        list(df),\n" +
    "        as.list(\n" +
    "          if (nrow(df) > 0)\n" +
    "            lapply(grouped_by, function(group_by_name) df[[group_by_name]][[1]])\n" +
    "          else\n" +
    "            NULL\n" +
    "        )\n" +
    "      )[0:closure_params]\n" +
    "\n" +
    "      worker_log(\"computing closure\")\n" +
    "      result <- do.call(closure, closure_args)\n" +
    "      worker_log(\"computed closure\")\n" +
    "\n" +
    "      if (!identical(class(result), \"data.frame\")) {\n" +
    "        worker_log(\"data.frame expected but \", class(result), \" found\")\n" +
    "        result <- data.frame(result)\n" +
    "      }\n" +
    "\n" +
    "      if (!is.data.frame(result)) stop(\"Result from closure is not a data.frame\")\n" +
    "    }\n" +
    "\n" +
    "    if (grouped) {\n" +
    "      new_column_values <- lapply(grouped_by, function(grouped_by_name) df[[grouped_by_name]][[1]])\n" +
    "      names(new_column_values) <- grouped_by\n" +
    "\n" +
    "      result <- do.call(\"cbind\", list(new_column_values, result))\n" +
    "    }\n" +
    "\n" +
    "    all_results <- rbind(all_results, result)\n" +
    "  }\n" +
    "\n" +
    "  if (!is.null(all_results) && nrow(all_results) > 0) {\n" +
    "    worker_log(\"updating \", nrow(all_results), \" rows\")\n" +
    "    all_data <- lapply(1:nrow(all_results), function(i) as.list(all_results[i,]))\n" +
    "\n" +
    "    worker_invoke(context, \"setResultArraySeq\", all_data)\n" +
    "    worker_log(\"updated \", nrow(all_results), \" rows\")\n" +
    "  } else {\n" +
    "    worker_log(\"found no rows in closure result\")\n" +
    "  }\n" +
    "\n" +
    "  worker_log(\"finished apply\")\n" +
    "}\n" +
    "\n" +
    "spark_worker_rlang_unserialize <- function() {\n" +
    "  rlang_unserialize <- core_get_package_function(\"rlang\", \"bytes_unserialise\")\n" +
    "  if (is.null(rlang_unserialize))\n" +
    "    core_get_package_function(\"rlanglabs\", \"bytes_unserialise\")\n" +
    "  else\n" +
    "    rlang_unserialize\n" +
    "}\n" +
    "\n" +
    "#' Extracts a bundle of dependencies required by \\code{spark_apply()}\n" +
    "#'\n" +
    "#' @param bundle_path Path to the bundle created using \\code{core_spark_apply_bundle()}\n" +
    "#' @param base_path Base path to use while extracting bundles\n" +
    "#'\n" +
    "#' @keywords internal\n" +
    "#' @export\n" +
    "worker_spark_apply_unbundle <- function(bundle_path, base_path) {\n" +
    "  extractPath <- file.path(base_path, core_spark_apply_unbundle_path())\n" +
    "  lockFile <- file.path(extractPath, \"sparklyr.lock\")\n" +
    "\n" +
    "  if (!dir.exists(extractPath)) dir.create(extractPath, recursive = TRUE)\n" +
    "\n" +
    "  if (length(dir(extractPath)) == 0) {\n" +
    "    worker_log(\"found that the unbundle path is empty, extracting:\", extractPath)\n" +
    "\n" +
    "    writeLines(\"\", lockFile)\n" +
    "    system2(\"tar\", c(\"-xf\", bundle_path, \"-C\", extractPath))\n" +
    "    unlink(lockFile)\n" +
    "  }\n" +
    "\n" +
    "  if (file.exists(lockFile)) {\n" +
    "    worker_log(\"found that lock file exists, waiting\")\n" +
    "    while (file.exists(lockFile)) {\n" +
    "      Sys.sleep(1000)\n" +
    "    }\n" +
    "    worker_log(\"completed lock file wait\")\n" +
    "  }\n" +
    "\n" +
    "  extractPath\n" +
    "}\n" +
    "spark_worker_connect <- function(\n" +
    "  sessionId,\n" +
    "  backendPort = 8880,\n" +
    "  config = list()) {\n" +
    "\n" +
    "  gatewayPort <- spark_config_value(config, \"sparklyr.worker.gateway.port\", backendPort)\n" +
    "\n" +
    "  gatewayAddress <- spark_config_value(config, \"sparklyr.worker.gateway.address\", \"localhost\")\n" +
    "  config <- list()\n" +
    "\n" +
    "  worker_log(\"is connecting to backend using port \", gatewayPort)\n" +
    "\n" +
    "  gatewayInfo <- spark_connect_gateway(gatewayAddress,\n" +
    "                                       gatewayPort,\n" +
    "                                       sessionId,\n" +
    "                                       config = config,\n" +
    "                                       isStarting = TRUE)\n" +
    "\n" +
    "  worker_log(\"is connected to backend\")\n" +
    "  worker_log(\"is connecting to backend session\")\n" +
    "\n" +
    "  tryCatch({\n" +
    "    # set timeout for socket connection\n" +
    "    timeout <- spark_config_value(config, \"sparklyr.backend.timeout\", 30 * 24 * 60 * 60)\n" +
    "    backend <- socketConnection(host = \"localhost\",\n" +
    "                                port = gatewayInfo$backendPort,\n" +
    "                                server = FALSE,\n" +
    "                                blocking = TRUE,\n" +
    "                                open = \"wb\",\n" +
    "                                timeout = timeout)\n" +
    "  }, error = function(err) {\n" +
    "    close(gatewayInfo$gateway)\n" +
    "\n" +
    "    stop(\n" +
    "      \"Failed to open connection to backend:\", err$message\n" +
    "    )\n" +
    "  })\n" +
    "\n" +
    "  worker_log(\"is connected to backend session\")\n" +
    "\n" +
    "  sc <- structure(class = c(\"spark_worker_connection\"), list(\n" +
    "    # spark_connection\n" +
    "    master = \"\",\n" +
    "    method = \"shell\",\n" +
    "    app_name = NULL,\n" +
    "    config = NULL,\n" +
    "    # spark_shell_connection\n" +
    "    spark_home = NULL,\n" +
    "    backend = backend,\n" +
    "    monitor = gatewayInfo$gateway,\n" +
    "    output_file = NULL\n" +
    "  ))\n" +
    "\n" +
    "  worker_log(\"created connection\")\n" +
    "\n" +
    "  sc\n" +
    "}\n" +
    "\n" +
    "connection_is_open.spark_worker_connection <- function(sc) {\n" +
    "  bothOpen <- FALSE\n" +
    "  if (!identical(sc, NULL)) {\n" +
    "    tryCatch({\n" +
    "      bothOpen <- isOpen(sc$backend) && isOpen(sc$monitor)\n" +
    "    }, error = function(e) {\n" +
    "    })\n" +
    "  }\n" +
    "  bothOpen\n" +
    "}\n" +
    "\n" +
    "worker_connection <- function(x, ...) {\n" +
    "  UseMethod(\"worker_connection\")\n" +
    "}\n" +
    "\n" +
    "worker_connection.spark_jobj <- function(x, ...) {\n" +
    "  x$connection\n" +
    "}\n" +
    "worker_invoke_method <- function(sc, static, object, method, ...)\n" +
    "{\n" +
    "  core_invoke_method(sc, static, object, method, ...)\n" +
    "}\n" +
    "\n" +
    "worker_invoke <- function(jobj, method, ...) {\n" +
    "  UseMethod(\"worker_invoke\")\n" +
    "}\n" +
    "\n" +
    "worker_invoke.shell_jobj <- function(jobj, method, ...) {\n" +
    "  worker_invoke_method(worker_connection(jobj), FALSE, jobj, method, ...)\n" +
    "}\n" +
    "\n" +
    "worker_invoke_static <- function(sc, class, method, ...) {\n" +
    "  worker_invoke_method(sc, TRUE, class, method, ...)\n" +
    "}\n" +
    "\n" +
    "worker_invoke_new <- function(sc, class, ...) {\n" +
    "  invoke_method(sc, TRUE, class, \"<init>\", ...)\n" +
    "}\n" +
    "worker_log_env <- new.env()\n" +
    "\n" +
    "worker_log_session <- function(sessionId) {\n" +
    "  assign('sessionId', sessionId, envir = worker_log_env)\n" +
    "}\n" +
    "\n" +
    "worker_log_format <- function(message, level = \"INFO\", component = \"RScript\") {\n" +
    "  paste(\n" +
    "    format(Sys.time(), \"%y/%m/%d %H:%M:%S\"),\n" +
    "    \" \",\n" +
    "    level,\n" +
    "    \" sparklyr: \",\n" +
    "    component,\n" +
    "    \" (\",\n" +
    "    worker_log_env$sessionId,\n" +
    "    \") \",\n" +
    "    message,\n" +
    "    sep = \"\")\n" +
    "}\n" +
    "\n" +
    "worker_log_level <- function(..., level) {\n" +
    "  if (is.null(worker_log_env$sessionId)) return()\n" +
    "\n" +
    "  args = list(...)\n" +
    "  message <- paste(args, sep = \"\", collapse = \"\")\n" +
    "  formatted <- worker_log_format(message, level)\n" +
    "  cat(formatted, \"\\n\")\n" +
    "}\n" +
    "\n" +
    "worker_log <- function(...) {\n" +
    "  worker_log_level(..., level = \"INFO\")\n" +
    "}\n" +
    "\n" +
    "worker_log_warning<- function(...) {\n" +
    "  worker_log_level(..., level = \"WARN\")\n" +
    "}\n" +
    "\n" +
    "worker_log_error <- function(...) {\n" +
    "  worker_log_level(..., level = \"ERROR\")\n" +
    "}\n" +
    ".worker_globals <- new.env(parent = emptyenv())\n" +
    "\n" +
    "spark_worker_main <- function(\n" +
    "  sessionId,\n" +
    "  backendPort = 8880,\n" +
    "  configRaw = NULL) {\n" +
    "\n" +
    "  spark_worker_hooks()\n" +
    "\n" +
    "  tryCatch({\n" +
    "    if (is.null(configRaw)) configRaw <- worker_config_serialize(list())\n" +
    "\n" +
    "    config <- worker_config_deserialize(configRaw)\n" +
    "\n" +
    "    if (config$debug) {\n" +
    "      worker_log(\"exiting to wait for debugging session to attach\")\n" +
    "\n" +
    "      # sleep for 1 day to allow long debugging sessions\n" +
    "      Sys.sleep(60*60*24)\n" +
    "      return()\n" +
    "    }\n" +
    "\n" +
    "    worker_log_session(sessionId)\n" +
    "    worker_log(\"is starting\")\n" +
    "\n" +
    "    sc <- spark_worker_connect(sessionId, backendPort, config)\n" +
    "    worker_log(\"is connected\")\n" +
    "\n" +
    "    spark_worker_apply(sc)\n" +
    "\n" +
    "  }, error = function(e) {\n" +
    "    worker_log_error(\"terminated unexpectedly: \", e$message)\n" +
    "    if (exists(\".stopLastError\", envir = .GlobalEnv)) {\n" +
    "      worker_log_error(\"collected callstack: \\n\", get(\".stopLastError\", envir = .worker_globals))\n" +
    "    }\n" +
    "    quit(status = -1)\n" +
    "  })\n" +
    "\n" +
    "  worker_log(\"finished\")\n" +
    "}\n" +
    "\n" +
    "spark_worker_hooks <- function() {\n" +
    "  unlock <- get(\"unlockBinding\")\n" +
    "  lock <- get(\"lockBinding\")\n" +
    "\n" +
    "  originalStop <- stop\n" +
    "  unlock(\"stop\",  as.environment(\"package:base\"))\n" +
    "  assign(\"stop\", function(...) {\n" +
    "    frame_names <- list()\n" +
    "    frame_start <- max(1, sys.nframe() - 5)\n" +
    "    for (i in frame_start:sys.nframe()) {\n" +
    "      current_call <- sys.call(i)\n" +
    "      frame_names[[1 + i - frame_start]] <- paste(i, \": \", paste(head(deparse(current_call), 5), collapse = \"\\n\"), sep = \"\")\n" +
    "    }\n" +
    "\n" +
    "    assign(\".stopLastError\", paste(rev(frame_names), collapse = \"\\n\"), envir = .worker_globals)\n" +
    "    originalStop(...)\n" +
    "  }, as.environment(\"package:base\"))\n" +
    "  lock(\"stop\",  as.environment(\"package:base\"))\n" +
    "}\n" +
    "do.call(spark_worker_main, as.list(commandArgs(trailingOnly = TRUE)))\n" +
    ""
}
