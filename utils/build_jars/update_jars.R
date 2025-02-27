
devtools::load_all(".")

# Downloads Scala compilers
download_scalac()

# Verifies, and installs, needed Spark versions
sparklyr_jar_verify_spark()

# Updates jar's
compile_package_jars()

# Embedded sources are the R functions that will be copied into the JARs.
# They are all placed inside the java/embedded_sources.R file. The source
# are all the R scripts in /R with a name containing "worker" or "core".
# Embedded sources are the key to how spark_apply() works.
spark_update_embedded_sources()



