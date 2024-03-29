% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/data_interface.R
\name{spark_read}
\alias{spark_read}
\title{Read file(s) into a Spark DataFrame using a custom reader}
\usage{
spark_read(sc, paths, reader, columns, packages = TRUE, ...)
}
\arguments{
\item{sc}{A \code{spark_connection}.}

\item{paths}{A character vector of one or more file URIs (e.g.,
c("hdfs://localhost:9000/file.txt", "hdfs://localhost:9000/file2.txt"))}

\item{reader}{A self-contained R function that takes a single file URI as
argument and returns the data read from that file as a data frame.}

\item{columns}{a named list of column names and column types of the resulting
data frame (e.g., list(column_1 = "integer", column_2 = "character")), or a
list of column names only if column types should be inferred from the data
(e.g., list("column_1", "column_2"), or NULL if column types should be
inferred and resulting data frame can have arbitrary column names}

\item{packages}{A list of R packages to distribute to Spark workers}

\item{...}{Optional arguments; currently unused.}
}
\description{
Run a custom R function on Spark workers to ingest data from one or more files
into a Spark DataFrame, assuming all files follow the same schema.
}
\examples{
\dontrun{

library(sparklyr)
sc <- spark_connect(
  master = "yarn",
  spark_home = "~/spark/spark-2.4.5-bin-hadoop2.7"
)

# This is a contrived example to show reader tasks will be distributed across
# all Spark worker nodes
spark_read(
  sc,
  rep("/dev/null", 10),
  reader = function(path) system("hostname", intern = TRUE),
  columns = c(hostname = "string")
) \%>\% sdf_collect()
}

}
\seealso{
Other Spark serialization routines: 
\code{\link{collect_from_rds}()},
\code{\link{spark_insert_table}()},
\code{\link{spark_load_table}()},
\code{\link{spark_read_avro}()},
\code{\link{spark_read_binary}()},
\code{\link{spark_read_csv}()},
\code{\link{spark_read_delta}()},
\code{\link{spark_read_image}()},
\code{\link{spark_read_jdbc}()},
\code{\link{spark_read_json}()},
\code{\link{spark_read_libsvm}()},
\code{\link{spark_read_orc}()},
\code{\link{spark_read_parquet}()},
\code{\link{spark_read_source}()},
\code{\link{spark_read_table}()},
\code{\link{spark_read_text}()},
\code{\link{spark_save_table}()},
\code{\link{spark_write_avro}()},
\code{\link{spark_write_csv}()},
\code{\link{spark_write_delta}()},
\code{\link{spark_write_jdbc}()},
\code{\link{spark_write_json}()},
\code{\link{spark_write_orc}()},
\code{\link{spark_write_parquet}()},
\code{\link{spark_write_source}()},
\code{\link{spark_write_table}()},
\code{\link{spark_write_text}()}
}
\concept{Spark serialization routines}
