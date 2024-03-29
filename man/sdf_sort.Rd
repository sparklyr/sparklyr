% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/sdf_interface.R
\name{sdf_sort}
\alias{sdf_sort}
\title{Sort a Spark DataFrame}
\usage{
sdf_sort(x, columns)
}
\arguments{
\item{x}{An object coercable to a Spark DataFrame.}

\item{columns}{The column(s) to sort by.}
}
\description{
Sort a Spark DataFrame by one or more columns, with each column
sorted in ascending order.
}
\seealso{
Other Spark data frames: 
\code{\link{sdf_copy_to}()},
\code{\link{sdf_distinct}()},
\code{\link{sdf_random_split}()},
\code{\link{sdf_register}()},
\code{\link{sdf_sample}()},
\code{\link{sdf_weighted_sample}()}
}
\concept{Spark data frames}
