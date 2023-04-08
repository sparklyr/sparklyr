# nocov start

is_win64 <- function() {
  Sys.getenv("PROCESSOR_ARCHITECTURE") == "AMD64" ||
    Sys.getenv("PROCESSOR_ARCHITEW6432") == "AMD64"
}

is_wow64 <- function() {
  Sys.getenv("PROCESSOR_ARCHITECTURE") == "x86" &&
    Sys.getenv("PROCESSOR_ARCHITEW6432") == "AMD64"
}

verify_msvcr100 <- function() {
  # determine location of MSVCR100.DLL
  systemRoot <- Sys.getenv("SystemRoot")
  systemDir <- ifelse(is_win64(), ifelse(is_wow64(), "sysnative", "system32"), "system32")
  msvcr100Path <- normalizePath(file.path(systemRoot, systemDir, "msvcr100.dll"),
    winslash = "/", mustWork = FALSE
  )
  haveMsvcr100 <- file.exists(msvcr100Path)
  if (!haveMsvcr100) {
    msvcr100Url <- ifelse(is_win64(),
      "https://www.microsoft.com/en-us/download/details.aspx?id=26999",
      "https://www.microsoft.com/download/en/details.aspx?id=8328"
    )
    stop("Running Spark on Windows requires the ",
      "Microsoft Visual C++ 2010 SP1 Redistributable Package. ",
      "Please download and install from: \n\n  ", msvcr100Url, "\n\n",
      "Then restart R after the installation completes", "\n",
      call. = FALSE
    )
  }
}

prepare_windows_environment <- function(sparkHome, sparkEnvironment = NULL) {
  verbose <- identical(getOption("sparkinstall.verbose"), TRUE)
  verboseMessage <- function(...) {
    if (verbose) message(...)
  }

  # don't do anything if aren't on windows
  if (.Platform$OS.type != "windows") {
    return()
  }

  # verify we have msvcr100
  verify_msvcr100()
  verboseMessage("Confirmed that msvcr100 is installed")

  # set HADOOP_HOME
  hivePath <- normalizePath(file.path(sparkHome, "tmp", "hive"), mustWork = FALSE)
  verboseMessage("HIVE_PATH set to ", hivePath)

  if (!dir.exists(hivePath)) {
    verboseMessage("HIVE_PATH does not exist")
    dir.create(hivePath, recursive = TRUE)
  }

  hadoopPath <- normalizePath(file.path(sparkHome, "tmp", "hadoop"), mustWork = FALSE)
  hadoopBinPath <- normalizePath(file.path(hadoopPath, "bin"), mustWork = FALSE)
  if (!dir.exists(hadoopPath)) {
    verboseMessage("HADOOP_HOME does not exist")

    dir.create(hadoopBinPath, recursive = TRUE)
  }
  verboseMessage("HADOOP_HOME exists under ", hadoopPath)

  if (is.null(sparkEnvironment)) {
    if (nchar(Sys.getenv("HADOOP_HOME")) == 0 ||
      Sys.getenv("HADOOP_HOME") != hadoopPath) {
      if (Sys.getenv("HADOOP_HOME") != hadoopPath) {
        warning("HADOOP_HOME was already but does not match current Spark installation")
      } else {
        verboseMessage("HADOOP_HOME environment variable not set")
      }

      output <- system2(
        "SETX", c("HADOOP_HOME", shQuote(hadoopPath)),
        stdout = if (verbose) TRUE else NULL
      )

      verboseMessage("HADOOP_HOME environment set with output ", output)
    } else {
      verboseMessage("HADOOP_HOME environment was already set to ", Sys.getenv("HADOOP_HOME"))
    }
  }
  else {
    # assign HADOOP_HOME to system2 environment which is more reliable than SETX
    sparkEnvironment$HADOOP_HOME <- hadoopPath
  }

  # pre-create the hive temp folder to manage permissions issues
  appUserTempDir <- normalizePath(
    file.path(Sys.getenv("LOCALAPPDATA"), "temp", Sys.info()[["login"]]),
    mustWork = FALSE
  )

  if (!dir.exists(appUserTempDir)) {
    verboseMessage("HIVE_TEMP does not exist")

    # create directory from using current user which will assign the right
    # permissions to execute in non-admin mode
    dir.create(appUserTempDir, recursive = TRUE)
  }
  verboseMessage("HIVE_TEMP exists under ", appUserTempDir)

  # form path to winutils.exe
  winutils <- normalizePath(file.path(hadoopBinPath, "winutils.exe"),
    mustWork = FALSE
  )

  # get a copy of winutils if we don't already have it
  if (!file.exists(winutils)) {
    verboseMessage("WINUTIL does not exist")

    winutilsSrc <- winutils_source_path()
    if (nzchar(winutilsSrc)) {
      file.copy(winutilsSrc, winutils)
    } else {
      stop_with_winutils_error(hadoopBinPath)
    }
  }
  verboseMessage("WINUTIL exists under ", winutils)

  # ensure correct permissions on hive path
  output <- system2(
    winutils,
    c("chmod", "777", shQuote(hivePath)),
    stdout = if (verbose) TRUE else NULL
  )

  verboseMessage("WINUTIL for CHMOD outputs ", output)

  output <- system2(
    winutils,
    c("ls", shQuote(hivePath)),
    stdout = TRUE
  )

  if (!is.null(output) && grepl("error", output)) {
    stop(output)
  }

  verboseMessage("WINUTIL for ls outputs ", output)
}


winutils_source_path <- function() {
  winutilsSrc <- ""

  # check for rstudio version of winutils
  rstudioWinutils <- Sys.getenv("RSTUDIO_WINUTILS", unset = NA)
  if (!is.na(rstudioWinutils)) {
    if (is_win64()) {
      rstudioWinutils <- file.path(rstudioWinutils, "x64")
    }
    rstudioWinutils <- file.path(rstudioWinutils, "winutils.exe")
    winutilsSrc <- normalizePath(rstudioWinutils, mustWork = FALSE)
  }

  winutilsSrc
}


stop_with_winutils_error <- function(hadoopBinPath) {
  if (is_win64()) {
    winutilsDownload <- "https://github.com/steveloughran/winutils/raw/master/hadoop-2.6.0/bin/"
  } else {
    winutilsDownload <- "https://code.google.com/archive/p/rrd-hadoop-win32/source/default/source"
  }

  stop(
    "\n\n",
    "To run Spark on Windows you need a copy of Hadoop winutils.exe:",
    "\n\n",
    "1. Download Hadoop winutils.exe from:",
    "\n\n",
    paste("  ", winutilsDownload),
    "\n\n",
    paste("2. Copy winutils.exe to", hadoopBinPath),
    "\n\n",
    "Alternatively, if you are using RStudio you can install the RStudio Preview Release,\n",
    "which includes an embedded copy of Hadoop winutils.exe:\n\n",
    "  https://www.posit.co/products/rstudio/download/preview/",
    "\n\n",
    call. = FALSE
  )
}
# nocov end
