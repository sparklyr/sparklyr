library(testthat)

context("ec2")

mock_ec2_cluster <- function() {
  list(
    accessKeyId = "A",
    secretAccessKey = "B",
    pemFile = "empty.pem",
    sparkDir = "/sparkdir",
    version = "1.6.1",
    hadoopVersion = "2.7",
    clusterName = "test",
    instanceType = "m3.medium",
    region = "us-west-1",
    installInfo = list()
  )
}

test_that("spark_Ec2_deploy produces expected command", {
  ci <- mock_ec2_cluster()
  operation <- spark_ec2_deploy(ci)

  expect_equal(
    operation$command,
    paste(
      "AWS_ACCESS_KEY_ID=A",
      "AWS_SECRET_ACCESS_KEY=B",
      "/sparkdir/ec2/spark-ec2",
      "--key-pair=empty",
      "--identity-file=empty.pem",
      "--region=us-west-1",
      "--instance-type=m3.medium",
      "--copy-aws-credentials",
      "-s 1 launch test"
    )
  )
})

