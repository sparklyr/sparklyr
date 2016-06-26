spark_install_windows_local <- function() {
  hivePath <- normalizePath("\\tmp\\hive")
  hadoopPath <- normalizePath("\\tmp\\hadoop")
  hadoopBinPath <- normalizePath(file.path(hadoopPath, "bin"))
  if (!dir.exists(hadoopPath)) {
    dir.create(hadoopBinPath, recursive = TRUE)
    message(paste("Created default hadoop bin directory under:", hadoopPath))
  }
  
  appUserTempDir <- normalizePath(file.path(rappdirs::app_dir("")$data(), "temp", Sys.info()[["login"]]))
  if (!dir.exists(appUserTempDir)) {
    # create directory from using current user which will assign the right permissions to execute in non-admin mode
    dir.create(appUserTempDir, recursive = FALSE)
  }
  
  if (!"HADOOP_HOME" %in% names(Sys.getenv())) {
    system2("SETX", c("HADOOP_HOME", hadoopPath), stdout = NULL)
    message(paste("Set HADOOP_HOME to:", hadoopPath))
  }
  
  vcRedistDownload <- "http://www.microsoft.com/download/en/details.aspx?id=13523"
  winutilsDownload <- "https://code.google.com/archive/p/rrd-hadoop-win32/source/default/source"
  if (!grepl("x64", version$platform)) {
    vcRedistDownload <- "http://www.microsoft.com/download/en/details.aspx?id=8328"
    winutilsDownload <- "https://github.com/steveloughran/winutils/tree/master/hadoop-2.6.0/bin"
  }
   
  message(
    "\n",
    "To complete the installation:",
    "\n\n",
    "1. Download and install Microsoft Visual C++ Redistributable for Visual Studio 2010:",
    "\n\n",
    paste("  ", vcRedistDownload),
    "\n\n",
    "2. Download winutils from:",
    "\n\n",
    paste("  ", winutilsDownload),
    "\n\n",
    paste("3. Copy winutils.exe to", hadoopBinPath),
    "\n\n",
    "4. Launch the command prompt and run:",
    "\n\n",
    paste0("   ", hadoopBinPath, "\\winutils.exe ", "chmod 777 ", hivePath)
  )
}