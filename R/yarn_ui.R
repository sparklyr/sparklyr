spark_connection_yarn_ui <- function(sc) {
  yarnui_url <- spark_config_value(sc$config, "sparklyr.web.yarn")
  if (!is.null(yarnui_url)) {
    yarnui_url
  }
  else {
    spark_ui <- spark_web(sc)

    domain_protocol <- regmatches(spark_ui, regexec("^http://|^https://", spark_ui))[[1]]

    domain_port <- strsplit(gsub("^http://|^https://", "", spark_ui), "/")[[1]][1]
    domain <- gsub(":[0-9]+$", "", domain_port)

    port <- "8088"
    webapp_address <- paste0(domain_protocol, domain, ":", port)

    rm_hostname <- spark_yarn_get_conf_property("yarn.resourcemanager.hostname", fails = FALSE)
    if (length(rm_hostname)) {
      webapp_address <- paste0(domain_protocol, rm_hostname, port)
    }

    rm_address <- spark_yarn_get_conf_property("yarn.resourcemanager.address", fails = FALSE)
    if (length(rm_address)) {
      webapp_address <- paste0(domain_protocol, gsub(":[0-9]+$", "", rm_address), ":", port)
    }

    webapp_address
  }
}
