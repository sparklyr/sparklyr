Sparkster
=======

A tool for managing Spark clusters in R.

## Installation

Sparkster is not yet on CRAN; it is currently available from GitHub. Make sure you have the devtools package installed, and then run:

```R
devtools::install_github("rstudio/sparkster")
```

## Example

To start a new 1-master 1-slave Spark cluster in EC2 run the following code:

```R
sparkster::start_ec2(access_key_id = "AAAAAAAAAAAAAAAAAAAA",
                     secret_access_key = "1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
                     pem_file = "sparkster.pem")
```

The `access_key_id`, `secret_access_key` and `pem_file` need to be retrieved from the AWS console.
