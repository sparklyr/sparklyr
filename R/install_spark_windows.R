spark_install_windows_local <- function() {
  hadoopPath <- "\\tmp\\hadoop"
  hadoopBinPath <- file.path(hadoopPath, "bin", fsep = "\\")
  if (!dir.exists(hadoopPath)) {
    dir.create(hadoopBinPath, recursive = TRUE)
    message(paste("Created default hadoop bin directory under:", hadoopPath))
  }
  
  if (!"HADOOP_HOME" %in% names(Sys.getenv())) {
    Sys.setenv("HADOOP_HOME" = hivePath)
    message(paste("Set HADOOP_HOME to:", hadoopPath))
  }
  
  vcRedistDownload <- "http://www.microsoft.com/download/en/details.aspx?id=13523"
  winutilsDownload <- "https://code.google.com/archive/p/rrd-hadoop-win32/source/default/source"
  if (!grepl("x64", version$platform)) {
    vcRedistDownload <- "http://www.microsoft.com/download/en/details.aspx?id=8328"
    winutilsDownload <- "https://github.com/steveloughran/winutils/tree/master/hadoop-2.6.0/bin"
  }
  
  message(
    "1. Download and install Microsoft Visual C++ Redistributable for Visual Studio 2015:",
    "\n\n",
    paste("  ", vcRedistDownload),
    "\n\n",
    "2. Download winutils from:",
    "\n\n",
    paste("  ", winutilsDownload),
    "\n\n",
    paste("3. Copy winutils.exe to", hadoopBinPath),
    "\n\n",
    "3. From the command prompt application run:",
    "\n\n",
    paste("   <path-to-file>\\winutils.exe chmod 777", hadoopPath),
    "\n\n",
    "References: ",
    "\n\n",
    "   https://hernandezpaul.wordpress.com/2016/01/24/apache-spark-installation-on-windows-10/"
  )
}