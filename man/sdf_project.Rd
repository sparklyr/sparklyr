% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/sdf_ml.R
\name{sdf_project}
\alias{sdf_project}
\title{Project features onto principal components}
\usage{
sdf_project(
  object,
  newdata,
  features = dimnames(object$pc)[[1]],
  feature_prefix = NULL,
  ...
)
}
\arguments{
\item{object}{A Spark PCA model object}

\item{newdata}{An object coercible to a Spark DataFrame}

\item{features}{A vector of names of columns to be projected}

\item{feature_prefix}{The prefix used in naming the output features}

\item{...}{Optional arguments; currently unused.}
}
\description{
Project features onto principal components
}
