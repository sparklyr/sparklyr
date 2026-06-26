# Package maintenance

- `build_jars` - Scala scripts and script that creates the JAR files. Source
`build_jars.R` to perform all needed operations.

- `spark_versions` - Contains the script that updates a file that lists the 
location to the available Spark versions. `spark_install()` uses that list 
to know where to download Spark from. The file is 
`inst/exdata/versions.json`. Run the script when new versions of Spark are 
available. Avoid updating the `versions.json` file manually.

- `coverage` - Functions-only helper for checking code coverage of a single R
script (leveraging the 1:1 R↔test alignment). Source `coverage.R` from the
package root, then call `coverage_lines("dplyr_verbs")` for a text annotation of
covered/uncovered lines, `coverage_report("dplyr_verbs")` for the interactive
HTML report, or `coverage_summary("dplyr_verbs")` for a one-line percentage.
Results are cached per file within the session.

- `archive` - Older scripts

