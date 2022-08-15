# Package maintenance

- `build_jars` - Scala scripts and script that creates the JAR files. Source
`build_jars.R` to perform all needed operations.

- `spark_versions` - Contains the script that updates a file that lists the 
location to the available Spark versions. `spark_install()` uses that list 
to know where to download Spark from. The file is 
`inst/exdata/versions.json`. Run the script when new versions of Spark are 
available. Avoid updating the `versions.json` file manually.

- `archive` - Older scripts

