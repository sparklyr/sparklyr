context("kubernetes config")

test_that("spark_kubernetes_config can generate correct config", {
  expect_equal(
    spark_config_kubernetes("k8s://https://192.168.99.100:8443", driver = "spark-driver",
                            forward = FALSE, fix_config = FALSE),
    list(
      spark.master = "k8s://https://192.168.99.100:8443",
      sparklyr.shell.master = "k8s://https://192.168.99.100:8443",
      "sparklyr.shell.deploy-mode" = "cluster",
      sparklyr.gateway.remote = TRUE,
      sparklyr.shell.name = "sparklyr",
      sparklyr.shell.class = "sparklyr.Shell",
      sparklyr.connect.timeout = 120,
      sparklyr.web.spark = "http://localhost:4040",
      sparklyr.shell.conf = c(
        "spark.kubernetes.container.image=spark:sparklyr",
        "spark.kubernetes.driver.pod.name=spark-driver",
        "spark.kubernetes.authenticate.driver.serviceAccountName=spark"
      ),
      sparklyr.gateway.routing = FALSE,
      sparklyr.app.jar = "local:///opt/sparklyr/sparklyr-2.3-2.11.jar",
      sparklyr.connect.aftersubmit = NULL,
      sparklyr.connect.ondisconnect = NULL,
      spark.home = spark_home_dir()
    )
  )
})
