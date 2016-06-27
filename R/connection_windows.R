
is_win64 <- function(){
  Sys.getenv("PROCESSOR_ARCHITECTURE") == "AMD64" ||
  Sys.getenv("PROCESSOR_ARCHITEW6432") == "AMD64"
}

verify_msvcr100 <- function() {
  # determine location of MSVCR100.DLL
  systemRoot <- Sys.getenv("SystemRoot")
  msvcr100Path <- normalizePath(file.path(systemRoot, "System32", "msvcr100.dll"),
                                winslash = "/", mustWork = FALSE)
  haveMsvcr100 <- file.exists(msvcr100Path)
  if (!haveMsvcr100) {
    msvcr100Url <- ifelse(is_win64(), 
                          "https://www.microsoft.com/download/en/details.aspx?id=13523",
                          "https://www.microsoft.com/download/en/details.aspx?id=8328")
    stop("Running Spark on Windows requires the ",
         "Microsoft Visual C++ 2010 SP1 Redistributable Package. ",
         "Please download and install from: \n\n  ", msvcr100Url, "\n\n", call. = FALSE)
  }
}

prepare_windows_environment <- function() {
  
  # don't do anything if aren't on windows
  if (.Platform$OS.type != "windows")
    return()
  
  # verify we have msvcr100
  verify_msvcr100()
  
  # set HADOOP_HOME
  hivePath <- normalizePath("\\tmp\\hive", mustWork = FALSE)
  if (!dir.exists(hivePath))
    dir.create(hivePath, recursive = TRUE)
  hadoopPath <- normalizePath("\\tmp\\hadoop", mustWork = FALSE)
  hadoopBinPath <- normalizePath(file.path(hadoopPath, "bin"), mustWork = FALSE)
  if (!dir.exists(hadoopPath)) {
    dir.create(hadoopBinPath, recursive = TRUE)
    message(paste("Created default hadoop bin directory under:", hadoopPath))
  }
  system2("SETX", c("HADOOP_HOME", hadoopPath), stdout = NULL)

  # pre-create the hive temp folder to manage permissions issues  
  appUserTempDir <- normalizePath(
    file.path(Sys.getenv("LOCALAPPDATA"), "temp", Sys.info()[["login"]]), 
    mustWork = FALSE
  )
  if (!dir.exists(appUserTempDir)) {
    # create directory from using current user which will assign the right
    # permissions to execute in non-admin mode
    dir.create(appUserTempDir, recursive = FALSE)
  }
  
  # form path to winutils.exe
  winutils <- normalizePath(file.path(hadoopBinPath, "winutils.exe"), 
                            mustWork = FALSE)
  
  # get a copy of winutils if we don't already have it
  if (!file.exists(winutils)) {
    winutilsSrc <- winutils_source_path()
    if (nzchar(winutilsSrc))
      file.copy(winutilsSrc, winutils)
    else
      stop_with_winutils_error(hadoopBinPath)
  }
  
  # ensure correct permissions on hive path
  system2(winutils, c("chmod", "777", hivePath))
}


winutils_source_path <- function() {
  
  # check for rstudio version of winutils
  rstudioWinutils <- Sys.getenv("RSTUDIO_WINUTILS", unset = NA)
  if (!is.na(rstudioWinutils)) {
    if (is_win64())
      rstudioWinutils <- file.path(rstudioWinutils, "x64")
    rstudioWinutils <- file.path(rstudioWinutils, "winutils.exe")
    normalizePath(rstudioWinutils, mustWork = FALSE)
  # use embedded version (NOTE: this branch will go away once we
  # drop the embedded version)
  } else {
    system.file("winutils", 
                paste0("winutils", ifelse(is_win64(), "64", "32"), ".dat"),
                package = "sparklyr")
  }
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
    "  https://www.rstudio.com/products/rstudio/download/preview/",
    "\n\n",
    call. = FALSE
  )
}



