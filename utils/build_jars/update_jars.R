
devtools::load_all(".")

# Downloads Scala compilers
download_scalac()

# Verifies, and installs, needed Spark versions
sparklyr_jar_verify_spark()

# Updates jar's
compile_package_jars()

# Updates embeddes sources
spark_update_embedded_sources(
  jars_to_skip = "sparklyr-1.5-2.10.jar"
)

