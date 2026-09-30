devtools::load_all(".")

# Downloads Scala compilers
download_scalac()

# Verifies, and installs, needed Spark versions
sparklyr_jar_verify_spark()

# Updates jar's
spec <- spark_default_compilation_spec()

# These are set based on current user's setup, update to your machines paths
# to run Java that supports Spark 4+ (java_17)  and Spark <4 (java_8)
java_8 <- "/Library/Internet Plug-Ins/JavaAppletPlugin.plugin/Contents/Home"
java_17 <- "/Library/Java/JavaVirtualMachines/openjdk-17.jdk/Contents/Home"

withr::with_envvar(
  list(JAVA_HOME = java_8),
  compile_package_jars(spec = spec[1:3])
)

withr::with_envvar(
  list(JAVA_HOME = java_17),
  compile_package_jars(spec = spec[4])
)

# Embedded sources are the R functions that will be copied into the JARs.
# They are all placed inside the java/embedded_sources.R file. The source
# are all the R scripts in /R with a name containing "worker" or "core".
# Embedded sources are the key to how spark_apply() works.

spark_update_embedded_sources()
