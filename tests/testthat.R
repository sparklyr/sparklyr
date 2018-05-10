Sys.setenv("R_TESTS" = "")
library(testthat)
library(sparklyr)

if (identical(Sys.getenv("NOT_CRAN"), "true")) {

  if (Sys.getenv("INSTALL_WINUTILS") == "true") {
    options(sparkinstall.verbose = TRUE)
    message("Installing winutils...")

    version <- Sys.getenv("SPARK_VERSION", unset = "2.2.0")
    hadoop_version <- if (version < "2.0.0") "2.6" else "2.7"
    spark_dir <- paste("spark-", version, "-bin-hadoop", hadoop_version, sep = "")
    winutils_dir <- file.path(Sys.getenv("LOCALAPPDATA"), "spark", spark_dir, "tmp", "hadoop", "bin", fsep = "\\")
    dir.create(winutils_dir, recursive = TRUE)
    winutils_path <- file.path(winutils_dir, "winutils.exe", fsep = "\\")

    download.file(
      "https://github.com/steveloughran/winutils/raw/master/hadoop-2.6.0/bin/winutils.exe",
      winutils_path
    )

    message("Installed winutils in ", winutils_path)
  }

  test_check("sparklyr")
  on.exit({ spark_disconnect_all() ; livy_service_stop() })
}
