
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
  winutils <-  normalizePath(file.path(hadoopBinPath, "winutils.exe"), 
                             mustWork = FALSE)
  
  # ensure that winutils.exe is on the hadoop bin path
  file.copy(system.file("inst", "winutils", 
                        paste0("winutils", ifelse(is_win64(), "64", "32"), ".dat")),
            winutils)
  
  # execute the file permission command
  system2(winutils, c("chmod", "777", hivePath))
}
