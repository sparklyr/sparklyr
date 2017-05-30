package SparkWorker

object Embedded {
  def sources: String = "" +
    "spark_worker_apply <- function(sc) {\n" +
    "  context <- worker_invoke_static(sc, \"SparkWorker.WorkerRDD\", \"getContext\")\n" +
    "  log(\"retrieved worker context\")\n" +
    "\n" +
    "  length <- worker_invoke(context, \"getSourceArrayLength\")\n" +
    "  log(\"found \", length, \" rows\")\n" +
    "\n" +
    "  data <- worker_invoke(context, \"getSourceArraySeq\")\n" +
    "  log(\"retrieved \", length(data), \" rows\")\n" +
    "\n" +
    "  closureRaw <- worker_invoke(context, \"getClosure\")\n" +
    "  closure <- unserialize(closureRaw)\n" +
    "\n" +
    "  data <- lapply(data, closure)\n" +
    "\n" +
    "  worker_invoke(context, \"setResultArraySeq\", data)\n" +
    "  log(\"updated \", length(data), \" rows\")\n" +
    "\n" +
    "  spark_split <- worker_invoke(context, \"finish\")\n" +
    "  log(\"finished apply\")\n" +
    "}\n" +
    "\n" +
    "spark_worker_collect <- function(sc) {\n" +
    "  collected <- invoke_static(sc, \"sparklyr.Utils\", \"collect\", sdf, separator$regexp)\n" +
    "\n" +
    "  transformed <- lapply(collected, function(e) {\n" +
    "    sdf_deserialize_column(e, sc)\n" +
    "  })\n" +
    "}\n" +
    "spark_worker_connect <- function(sessionId) {\n" +
    "  gatewayPort <- \"8880\"\n" +
    "  gatewayAddress <- \"localhost\"\n" +
    "\n" +
    "  log(\"is connecting to backend using port \", gatewayPort)\n" +
    "\n" +
    "  gatewayInfo <- spark_connect_gateway(gatewayAddress,\n" +
    "                                       gatewayPort,\n" +
    "                                       sessionId,\n" +
    "                                       config = config,\n" +
    "                                       isStarting = TRUE)\n" +
    "\n" +
    "  log(\"is connected to backend\")\n" +
    "  log(\"is connecting to backend session\")\n" +
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
    "    abort_shell(\n" +
    "      paste(\"Failed to open connection to backend:\", err$message),\n" +
    "      spark_submit_path,\n" +
    "      shell_args,\n" +
    "      output_file,\n" +
    "      error_file\n" +
    "    )\n" +
    "  })\n" +
    "\n" +
    "  log(\"is connected to backend session\")\n" +
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
    "  log(\"created connection\")\n" +
    "\n" +
    "  sc\n" +
    "}\n" +
    "\n" +
    "jobj_subclass.shell_backend <- function(con) {\n" +
    "  \"worker_jobj\"\n" +
    "}\n" +
    "connection_is_open <- function(sc) {\n" +
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
    "  sparklyr:::writeInt(gateway, spark_gateway_commands()[[\"GetPorts\"]])\n" +
    "  sparklyr:::writeInt(gateway, sessionId)\n" +
    "  sparklyr:::writeInt(gateway, if (isStarting) waitSeconds else 0)\n" +
    "\n" +
    "  backendSessionId <- NULL\n" +
    "  redirectGatewayPort <- NULL\n" +
    "\n" +
    "  commandStart <- Sys.time()\n" +
    "  while(length(backendSessionId) == 0 && commandStart + waitSeconds > Sys.time()) {\n" +
    "    backendSessionId <- sparklyr:::readInt(gateway)\n" +
    "    Sys.sleep(0.1)\n" +
    "  }\n" +
    "\n" +
    "  redirectGatewayPort <- sparklyr:::readInt(gateway)\n" +
    "  backendPort <- sparklyr:::readInt(gateway)\n" +
    "\n" +
    "  if (length(backendSessionId) == 0 || length(redirectGatewayPort) == 0 || length(backendPort) == 0) {\n" +
    "    if (isStarting)\n" +
    "      stop(\"sparklyr gateway did not respond while retrieving ports information after \", waitSeconds, \" seconds\")\n" +
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
    "    gatewayPortsQuery <- query_gateway_for_port(gateway, sessionId, config, isStarting)\n" +
    "    if (is.null(gatewayPortsQuery) && !isStarting) {\n" +
    "      close(gateway)\n" +
    "      return(NULL)\n" +
    "    }\n" +
    "\n" +
    "    redirectGatewayPort <- gatewayPortsQuery$redirectGatewayPort\n" +
    "    backendPort <- gatewayPortsQuery$backendPort\n" +
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
    "\n" +
    "invoke_method <- function(sc, static, object, method, ...)\n" +
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
    "  returnStatus <- readInt(backend)\n" +
    "  if (length(returnStatus) == 0)\n" +
    "    stop(\"No status is returned. The sparklyr backend might have failed.\")\n" +
    "  if (returnStatus != 0) {\n" +
    "    # get error message from backend and report to R\n" +
    "    msg <- readString(backend)\n" +
    "    withr::with_options(list(\n" +
    "      warning.length = 8000\n" +
    "    ), {\n" +
    "      if (nzchar(msg))\n" +
    "        stop(msg, call. = FALSE)\n" +
    "      else {\n" +
    "        # read the spark log\n" +
    "        msg <- read_spark_log_error(sc)\n" +
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
    "worker_invoke <- function(jobj, method, ...) {\n" +
    "  UseMethod(\"worker_invoke\")\n" +
    "}\n" +
    "\n" +
    "worker_invoke.worker_jobj <- function(jobj, method, ...) {\n" +
    "  invoke_method(worker_connection(jobj), FALSE, jobj, method, ...)\n" +
    "}\n" +
    "\n" +
    "worker_invoke_static <- function(sc, class, method, ...) {\n" +
    "  invoke_method(sc, TRUE, class, method, ...)\n" +
    "}\n" +
    "\n" +
    "worker_invoke_new <- function(sc, class, ...) {\n" +
    "  invoke_method(sc, TRUE, class, \"<init>\", ...)\n" +
    "}\n" +
    "spark_jobj <- function(x, ...) {\n" +
    "  UseMethod(\"spark_jobj\")\n" +
    "}\n" +
    "\n" +
    "spark_jobj.default <- function(x, ...) {\n" +
    "  stop(\"Unable to retrieve a spark_jobj from object of class \",\n" +
    "       paste(class(x), collapse = \" \"), call. = FALSE)\n" +
    "}\n" +
    "\n" +
    "spark_jobj.spark_jobj <- function(x, ...) {\n" +
    "  x\n" +
    "}\n" +
    "\n" +
    "print.spark_jobj <- function(x, ...) {\n" +
    "  print_jobj(worker_connection(x), x, ...)\n" +
    "}\n" +
    "\n" +
    "print_jobj <- function(sc, jobj, ...) {\n" +
    "  UseMethod(\"print_jobj\")\n" +
    "}\n" +
    "\n" +
    ".validJobjs <- new.env(parent = emptyenv())\n" +
    "\n" +
    ".toRemoveJobjs <- new.env(parent = emptyenv())\n" +
    "\n" +
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
    "    class <- worker_invoke(jobj, \"getClass\")\n" +
    "    if (inherits(class, \"spark_jobj\"))\n" +
    "      class <- worker_invoke(class, \"toString\")\n" +
    "  }, error = function(e) {\n" +
    "  })\n" +
    "  tryCatch({\n" +
    "    repr <- worker_invoke(jobj, \"toString\")\n" +
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
    "  if (!connection_is_open(worker_connection(jobj)))\n" +
    "    return(jobj)\n" +
    "\n" +
    "  class <- worker_invoke(jobj, \"getClass\")\n" +
    "\n" +
    "  cat(\"Fields:\\n\")\n" +
    "  fields <- worker_invoke(class, \"getDeclaredFields\")\n" +
    "  lapply(fields, function(field) { print(field) })\n" +
    "\n" +
    "  cat(\"Methods:\\n\")\n" +
    "  methods <- worker_invoke(class, \"getDeclaredMethods\")\n" +
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
    "log_env <- new.env()\n" +
    "\n" +
    "log_session <- function(sessionId) {\n" +
    "  assign('sessionId', sessionId, envir = log_env)\n" +
    "}\n" +
    "\n" +
    "log_format <- function(message, level = \"INFO\") {\n" +
    "  paste(\n" +
    "    format(Sys.time(), \"%y/%m/%d %H:%M:%S\"),\n" +
    "    \" \",\n" +
    "    level,\n" +
    "    \" sparklyr: RScript (\",\n" +
    "    log_env$sessionId,\n" +
    "    \") \",\n" +
    "    message,\n" +
    "    sep = \"\")\n" +
    "}\n" +
    "\n" +
    "log_level <- function(..., level) {\n" +
    "  args = list(...)\n" +
    "  message <- paste(args, sep = \"\", collapse = \"\")\n" +
    "  formatted <- log_format(message, level)\n" +
    "  cat(formatted, \"\\n\")\n" +
    "}\n" +
    "\n" +
    "log <- function(...) {\n" +
    "  log_level(..., level = \"INFO\")\n" +
    "}\n" +
    "\n" +
    "log_warning<- function(...) {\n" +
    "  log_level(..., level = \"WARN\")\n" +
    "}\n" +
    "\n" +
    "log_error <- function(...) {\n" +
    "  log_level(..., level = \"ERROR\")\n" +
    "}\n" +
    "\n" +
    "unlockBinding(\"stop\",  as.environment(\"package:base\"))\n" +
    "assign(\"stop\", function(...) {\n" +
    "  log_error(...)\n" +
    "  quit(status = -1)\n" +
    "}, as.environment(\"package:base\"))\n" +
    "lockBinding(\"stop\",  as.environment(\"package:base\"))\n" +
    "spark_worker_main <- function(sessionId) {\n" +
    "  log_session(sessionId)\n" +
    "  log(\"is starting\")\n" +
    "\n" +
    "  tryCatch({\n" +
    "\n" +
    "    sc <- spark_worker_connect(sessionId)\n" +
    "    log(\"is connected\")\n" +
    "\n" +
    "    spark_worker_apply(sc)\n" +
    "\n" +
    "  }, error = function(e) {\n" +
    "      stop(\"terminated unexpectedly: \", e)\n" +
    "  })\n" +
    "\n" +
    "  log(\"finished\")\n" +
    "}\n" +
    "spark_config_value <- function(config, property, value) {\n" +
    "  value\n" +
    "}\n" +
    "getSerdeType <- function(object) {\n" +
    "  type <- class(object)[[1]]\n" +
    "\n" +
    "  if (type != \"list\") {\n" +
    "    type\n" +
    "  } else {\n" +
    "    # Check if all elements are of same type\n" +
    "    elemType <- unique(sapply(object, function(elem) { getSerdeType(elem) }))\n" +
    "    if (length(elemType) <= 1) {\n" +
    "      \"array\"\n" +
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
    "  if (type %in% c(\"integer\", \"character\", \"logical\", \"double\", \"numeric\")) {\n" +
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
    "spark_worker_main(commandArgs(trailingOnly = TRUE)[1])\n" +
    ""
}
