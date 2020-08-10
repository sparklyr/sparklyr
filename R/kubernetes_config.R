spark_config_kubernetes_forward_init <- function(
                                                 driver,
                                                 timeout,
                                                 ports) {
  Sys.sleep(timeout)
  system2(
    "kubectl",
    c("port-forward", driver, ports),
    wait = FALSE
  )
}

spark_config_kubernetes_terminal_id <- function() {
  id <- NULL
  for (terminal in rstudioapi::terminalList()) {
    terminal <- rstudioapi::terminalContext(terminal)
    if (identical(terminal$caption, "spark kubernetes")) {
      id <- terminal$handle
    }
  }

  if (identical(id, NULL)) {
    id <- rstudioapi::terminalCreate("spark kubernetes")
  }

  id
}

spark_config_kubernetes_forward_init_message <- function(
                                                         driver,
                                                         timeout,
                                                         ports) {
  message("Please enable port forwarding from your terminal:")
  message(paste("kubectl port-forward", driver, paste(ports, collapse = " ")))

  Sys.sleep(timeout)
}

spark_config_kubernetes_forward_init_terminal <- function(
                                                          driver,
                                                          timeout,
                                                          ports) {
  Sys.sleep(timeout)

  id <- spark_config_kubernetes_terminal_id()
  command <- paste("kubectl port-forward", driver, paste(ports, collapse = " "), "\r\n")

  rstudioapi::terminalSend(id, command)
}

spark_config_kubernetes_forward_cleanup <- function(
                                                    driver) {
  if (identical(.Platform$OS.type, "windows")) {
    if (rstudioapi::hasFun("terminalKill")) {
      id <- spark_config_kubernetes_terminal_id()
      rstudioapi::terminalKill(id)
    }
    else {
      message("Disconnected, please terminate 'kubectl port-forward'")
    }
  }
  else {
    system2("pkill", "kubectl")
  }
}

#' Kubernetes Configuration
#'
#' Convenience function to initialize a Kubernetes configuration instead
#' of \code{spark_config()}, exposes common properties to set in Kubernetes
#' clusters.
#'
#' @param master Kubernetes url to connect to, found by running \code{kubectl cluster-info}.
#' @param version The version of Spark being used.
#' @param image Container image to use to launch Spark and sparklyr. Also known
#'   as \code{spark.kubernetes.container.image}.
#' @param driver Name of the driver pod. If not set, the driver pod name is set
#'   to "sparklyr" suffixed by id to avoid name conflicts. Also known as
#'   \code{spark.kubernetes.driver.pod.name}.
#' @param account Service account that is used when running the driver pod. The driver
#'   pod uses this service account when requesting executor pods from the API
#'   server. Also known as \code{spark.kubernetes.authenticate.driver.serviceAccountName}.
#' @param jars Path to the sparklyr jars; either, a local path inside the container
#'   image with the sparklyr jars copied when the image was created or, a path
#'   accesible by the container where the sparklyr jars were copied. You can find
#'   a path to the sparklyr jars by running \code{system.file("java/", package = "sparklyr")}.
#' @param forward Should ports used in sparklyr be forwarded automatically through Kubernetes?
#'   Default to \code{TRUE} which runs \code{kubectl port-forward} and \code{pkill kubectl}
#'   on disconnection.
#' @param executors Number of executors to request while connecting.
#' @param conf A named list of additional entries to add to \code{sparklyr.shell.conf}.
#' @param timeout Total seconds to wait before giving up on connection.
#' @param ports Ports to forward using kubectl.
#' @param fix_config Should the spark-defaults.conf get fixed? \code{TRUE} for Windows.
#' @param ... Additional parameters, currently not in use.
#'
#' @export
spark_config_kubernetes <- function(
                                    master,
                                    version = "2.3.2",
                                    image = "spark:sparklyr",
                                    driver = random_string("sparklyr-"),
                                    account = "spark",
                                    jars = "local:///opt/sparklyr",
                                    forward = TRUE,
                                    executors = NULL,
                                    conf = NULL,
                                    timeout = 120,
                                    ports = c(8880, 8881, 4040),
                                    fix_config = identical(.Platform$OS.type, "windows"),
                                    ...) {
  args <- list(...)

  submit_function <- NULL
  disconnect_function <- NULL

  if (!identical(args$jar, NULL)) {
    jar <- args$jar
  }
  else {
    jar_version <- paste(strsplit(version, "\\.")[[1]][1:2], collapse = ".")
    jar <- file.path(
      jars,
      basename(spark_default_app_jar(version))
    )
  }

  if (fix_config) {
    defaults <- file.path(spark_install_find(version)$sparkConfDir, "spark-defaults.conf")
    lines <- readLines(defaults)
    lines <- gsub("^spark.local.dir", "# spark.local.dir", lines)
    lines <- gsub("^spark.sql.warehouse.dir", "# spark.sql.warehouse.dir", lines)
    writeLines(lines, defaults)
  }

  if (forward) {
    ports <- paste(ports, ports, sep = ":")
    if (identical(.Platform$OS.type, "windows")) {
      if (rstudioapi::hasFun("terminalCreate")) {
        submit_function <- function() spark_config_kubernetes_forward_init_terminal(driver, timeout / 2, ports)
      }
      else {
        submit_function <- function() spark_config_kubernetes_forward_init_message(driver, timeout / 2, ports)
      }
    }
    else {
      submit_function <- function() spark_config_kubernetes_forward_init(driver, timeout / 2, ports)
    }
    disconnect_function <- function() spark_config_kubernetes_forward_cleanup(driver)
  }

  list(
    spark.master = master,
    sparklyr.shell.master = master,
    "sparklyr.shell.deploy-mode" = "cluster",
    sparklyr.gateway.remote = TRUE,
    sparklyr.shell.name = "sparklyr",
    sparklyr.shell.class = "sparklyr.Shell",
    sparklyr.connect.timeout = timeout,
    sparklyr.web.spark = "http://localhost:4040",
    sparklyr.shell.conf = c(
      paste("spark.kubernetes.container.image", image, sep = "="),
      paste("spark.kubernetes.driver.pod.name", driver, sep = "="),
      paste("spark.kubernetes.authenticate.driver.serviceAccountName", account, sep = "="),
      if (!identical(executors, NULL)) paste("spark.executor.instances", executors, sep = "=") else NULL,
      if (!identical(conf, NULL)) paste(names(conf), conf, sep = "=") else NULL
    ),
    sparklyr.gateway.routing = FALSE,
    sparklyr.app.jar = jar,
    sparklyr.connect.aftersubmit = submit_function,
    sparklyr.connect.ondisconnect = disconnect_function,
    spark.home = spark_home_dir()
  )
}
