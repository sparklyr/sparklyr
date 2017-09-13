spark_yarn_cluster_get_conf_property <- function(property) {
  confDir <- Sys.getenv("YARN_CONF_DIR")
  if (nchar(confDir) == 0) {

    # some systems don't set YARN_CONF_DIR but do set HADOOP_CONF_DIR
    confDir <- Sys.getenv("HADOOP_CONF_DIR")
    if (nchar(confDir) == 0) {
      stop("Yarn Cluster mode requires YARN_CONF_DIR or HADOOP_CONF_DIR to be set.")
    }
  }

  yarnSite <- file.path(confDir, "yarn-site.xml")
  if (!file.exists(yarnSite)) {
    stop("Yarn Cluster mode requires yarn-site.xml to exist under YARN_CONF_DIR")
  }

  yarnSiteXml <- xml2::read_xml(yarnSite)

  yarnPropertyValue <- xml2::xml_text(xml2::xml_find_all(
      yarnSiteXml,
      paste0("//name[.='", property, "']/parent::property/value")
    )
  )

  yarnPropertyValue
}

spark_yarn_cluster_get_app_property <- function(config, start_time, rm_webapp, property) {
  resourceManagerQuery <- paste0(
    "http",
    "://",
    rm_webapp,
    "/ws/v1/cluster/apps?startedTimeBegin=",
    start_time,
    "&applicationType=SPARK"
  )

  waitSeconds <- spark_config_value(config, "sparklyr.yarn.cluster.start.timeout", 60)
  commandStart <- Sys.time()
  propertyValue <- NULL

  while(length(propertyValue) == 0 && commandStart + waitSeconds > Sys.time()) {
    resourceManagerResponce <- httr::GET(resourceManagerQuery)
    yarnApps <- httr::content(resourceManagerResponce)

    newSparklyrApps <- Filter(function(e) grepl("sparklyr.*", e[[1]]$name), yarnApps$apps)

    if (length(newSparklyrApps) > 1) {
      stop("Multiple sparklyr apps submitted at once to this yarn cluster, aborting, please retry")
    }

    newSparklyrApp <- newSparklyrApps[[1]][[1]]
    if (property %in% names(newSparklyrApp)) {
      propertyValue <- newSparklyrApp[[property]]
    }
    else {
      Sys.sleep(1)
    }
  }

  propertyValue
}

spark_yarn_cluster_resource_manager_is_online <- function(rm_webapp) {
  rmQuery <- paste0(
    "http",
    "://",
    rm_webapp,
    "/ws/v1/cluster/info"
  )

  tryCatch({
    !httr::http_error(rmQuery)
  }, error = function(err) {
    FALSE
  })
}

spark_yarn_cluster_get_resource_manager_webapp <- function() {
  rmHighAvailability <- spark_yarn_cluster_get_conf_property("yarn.resourcemanager.ha.enabled")
  rmHighAvailability <- length(rmHighAvailability) > 0 && grepl("TRUE", rmHighAvailability, ignore.case = TRUE)

  mainRMWebapp <- "yarn.resourcemanager.webapp.address"
  if (rmHighAvailability) {
    rmHighAvailabilityId <- spark_yarn_cluster_get_conf_property("yarn.resourcemanager.ha.id")

    rmHighAvailabilityIds <- spark_yarn_cluster_get_conf_property("yarn.resourcemanager.ha.rm-ids")
    rmHighAvailabilityIds <- strsplit(rmHighAvailabilityIds, ",")[[1]]
    rmHighAvailabilityIds <- rmHighAvailabilityIds[rmHighAvailabilityIds != rmHighAvailabilityId]
    rmHighAvailabilityIds <- c(rmHighAvailabilityId, rmHighAvailabilityIds)

    mainRMWebapp <- NULL
    propCandidates <- c(
      "yarn.resourcemanager.webapp.address.",
      "yarn.resourcemanager.admin.address."
    )

    for (propCandidate in propCandidates) {
      for (rmId in rmHighAvailabilityIds) {
        rmCandidate <- paste0(propCandidate, rmId)
        rmCandidateValue <- spark_yarn_cluster_get_conf_property(rmCandidate)

        if (spark_yarn_cluster_resource_manager_is_online(rmCandidateValue)) {
          mainRMWebapp <- rmCandidate
          break;
        }
      }
    }

    if (is.null(mainRMWebapp)) {
      stop("Failed to find online resource manager under High Availability cluster.")
    }
  }

  mainRMWebappValue <- spark_yarn_cluster_get_conf_property(mainRMWebapp)

  if (is.null(mainRMWebappValue)) {
    stop("Failed to retrieve ", mainRMWebapp, " from yarn-site.xml")
  }

  mainRMWebappValue
}

spark_yarn_cluster_get_gateway <- function(config, start_time) {
  resourceManagerWebapp <- spark_yarn_cluster_get_resource_manager_webapp()

  if (length(resourceManagerWebapp) == 0) {
    stop("Yarn Cluster mode uses `yarn.resourcemanager.webapp.address` but is not present in yarn-site.xml")
  }

  amHostHttpAddress <- spark_yarn_cluster_get_app_property(
    config, start_time,
    resourceManagerWebapp,
    "amHostHttpAddress")

  if (is.null(amHostHttpAddress)) {
    stop("Failed to retrieve new sparklyr yarn application from ", resourceManagerWebapp)
  }

  strsplit(amHostHttpAddress, ":")[[1]][[1]]
}
