library(sparklyr)

rsApiUpdateDialog <- function(code) {
  if (exists(".rs.api.updateDialog")) {
    updateDialog <- get(".rs.api.updateDialog")
    updateDialog(code = code)
  }
}

rsApiShowDialog <- function(title, message, url = "") {
  if (exists(".rs.api.showDialog")) {
    showDialog <- get(".rs.api.showDialog")
    showDialog(title, message, url)
  }
}

rsApiShowPrompt <- function(title, message, default) {
  if (exists(".rs.api.showPrompt")) {
    showPrompt <- get(".rs.api.showPrompt")
    showPrompt(title, message, default)
  }
}

rsApiShowQuestion <- function(title, message, ok, cancel) {
  if (exists(".rs.api.showQuestion")) {
    showPrompt <- get(".rs.api.showQuestion")
    showPrompt(title, message, ok, cancel)
  }
}

rsApiReadPreference <- function(name, default) {
  if (exists(".rs.api.readPreference")) {
    readPreference <- get(".rs.api.readPreference")
    value <- readPreference(name)
    if (is.null(value)) default else value
  }
}

rsApiWritePreference <- function(name, value) {
  if (!is.character(value)) {
    stop("Only character preferences are supported")
  }

  if (exists(".rs.api.writePreference")) {
    writePreference <- get(".rs.api.writePreference")
    writePreference(name, value)
  }
}

rsApiVersionInfo <- function() {
  if (exists(".rs.api.versionInfo")) {
    versionInfo <- get(".rs.api.versionInfo")
    versionInfo()
  }
}

is_java_available <- function() {
  nzchar(spark_get_java())
}

spark_home <- function() {
  home <- Sys.getenv("SPARK_HOME", unset = NA)
  if (is.na(home))
    home <- NULL
  home
}

spark_ui_avaliable_versions <- function() {
  tryCatch({
    spark_available_versions(show_hadoop = TRUE, show_minor = TRUE)
  }, error = function(e) {
    warning(e)
    spark_installed_versions()[,c("spark","hadoop")]
  })
}

spark_ui_spark_choices <- function() {
  availableVersions <- spark_ui_avaliable_versions()
  selected <- spark_default_version()[["spark"]]
  choiceValues <- unique(availableVersions[["spark"]])

  choiceNames <- choiceValues
  choiceNames <- lapply(
    choiceNames,
    function(e) if (e == selected) paste(e, "(Default)") else e
  )

  names(choiceValues) <- choiceNames

  choiceValues
}

spark_ui_hadoop_choices <- function(sparkVersion) {
  availableVersions <- spark_ui_avaliable_versions()

  selected <- spark_install_find(version = sparkVersion, installed_only = FALSE)$hadoopVersion

  choiceValues <- unique(availableVersions[availableVersions$spark == sparkVersion,][["hadoop"]])
  choiceNames <- choiceValues
  choiceNames <- lapply(
    choiceNames,
    function(e) if (length(selected) > 0 && e == selected) paste(e, "(Default)") else e
  )

  names(choiceValues) <- choiceNames

  choiceValues
}

spark_ui_default_connections <- function() {
  getOption(
    "sparklyr.ui.connections",
    getOption("rstudio.spark.connections")
  )
}

#' @import rstudioapi
connection_spark_ui <- function() {
  elementSpacing <- if (.Platform$OS.type == "windows") 2 else 7

  tags$div(
    tags$head(
      tags$style(
        HTML(paste("
          body {
            background: none;

            font-family : \"Lucida Sans\", \"DejaVu Sans\", \"Lucida Grande\", \"Segoe UI\", Verdana, Helvetica, sans-serif;
            font-size : 12px;
            -ms-user-select : none;
            -moz-user-select : none;
            -webkit-user-select : none;
            user-select : none;

            margin: 0;
            margin-top: 7px;
          }

          select {
            background: #FFF;
          }

          .shiny-input-container {
            min-width: 100%;
            margin-bottom: ", elementSpacing, "px;
          }

          .shiny-input-container > .control-label {
            display: table-cell;
            width: 195px;
          }

          .shiny-input-container > div {
            display: table-cell;
            width: 300px;
          }

          #shiny-disconnected-overlay {
            display: none;
          }
        ", sep = ""))
      )
    ),
    div(style = "table-row",
        selectInput(
          "master",
          "Master:",
          choices = c(
            list("local" = "local"),
            spark_ui_default_connections(),
            list("Cluster..." = "cluster")
          ),
          selectize = FALSE
        ),
        selectInput(
          "dbinterface",
          "DB Interface:",
          choices = c(
            "dplyr" = "dplyr",
            "(None)" = "none"
          ),
          selectize = FALSE,
          selected = rsApiReadPreference("sparklyr_dbinterface", "dplyr")
        )
    ),
    div(
      style = paste("display: table-row; height: 10px")
    ),
    conditionalPanel(
      condition = "!output.notShowVersionsUi",
      div(style = "table-row",
          selectInput(
            "sparkversion",
            "Spark version:",
            choices = spark_ui_spark_choices(),
            selected = spark_default_version()$spark,
            selectize = FALSE
          ),
          selectInput(
            "hadoopversion",
            "Hadoop version:",
            choices = spark_ui_hadoop_choices(spark_default_version()$spark),
            selected = spark_default_version()$hadoop,
            selectize = FALSE
          )
      )
    )
  )
}

connection_spark_server <- function(input, output, session) {
  hasDefaultSparkVersion <- reactive({
    input$sparkversion == spark_default_version()$spark
  })

  hasDefaultHadoopVersion <- reactive({
    input$hadoopversion == spark_default_version()$hadoop
  })

  output$notShowVersionsUi <- reactive({
    !identical(spark_home(), NULL)
  })

  userInstallPreference <- NULL
  checkUserInstallPreference <- function(master, sparkSelection, hadoopSelection, prompt) {
    if (identical(master, "local") &&
        identical(rsApiVersionInfo()$mode, "desktop") &&
        identical(spark_home(), NULL)) {

      installed <- spark_installed_versions()
      isInstalled <- nrow(installed[installed$spark == sparkSelection & installed$hadoop == hadoopSelection, ])

      if (!isInstalled) {
        if (prompt && identical(userInstallPreference, NULL)) {
          userInstallPreference <<- rsApiShowQuestion(
            "Install Spark Components",
            paste(
              "Spark ",
              sparkSelection,
              " for Hadoop ",
              hadoopSelection,
              " is not currently installed.",
              "\n\n",
              "Do you want to install this version of Spark?",
              sep = ""
            ),
            ok = "Install",
            cancel = "Cancel"
          )

          userInstallPreference
        }
        else if (identical(userInstallPreference, NULL)) {
          FALSE
        }
        else {
          userInstallPreference
        }
      }
      else {
        FALSE
      }
    }
    else {
      FALSE
    }
  }

  generateCode <- function(master, dbInterface, sparkVersion, hadoopVersion, installSpark) {
    paste(
      "library(sparklyr)\n",
      if(dbInterface == "dplyr") "library(dplyr)\n" else "",
      if(installSpark)
        paste(
          "spark_install(version = \"",
          sparkVersion,
          "\", hadoop_version = \"",
          hadoopVersion,
          "\")\n",
          sep = ""
        )
      else "",
      "sc ",
      "<- ",
      "spark_connect(master = \"",
      master,
      "\"",
      if (!hasDefaultSparkVersion())
        paste(
          ", version = \"",
          sparkVersion,
          "\"",
          sep = ""
        )
      else "",
      if (!hasDefaultHadoopVersion())
        paste(
          ", hadoop_version = \"",
          hadoopVersion,
          "\"",
          sep = ""
        )
      else "",
      ")",
      sep = ""
    )
  }

  stateValuesReactive <- reactiveValues(codeInvalidated = 1)

  codeReactive <- reactive({
    master <- input$master
    dbInterface <- input$dbinterface
    sparkVersion <- input$sparkversion
    hadoopVersion <- input$hadoopversion
    codeInvalidated <- stateValuesReactive$codeInvalidated

    installSpark <- checkUserInstallPreference(master, sparkVersion, hadoopVersion, FALSE)

    generateCode(master, dbInterface, sparkVersion, hadoopVersion, installSpark)
  })

  installLater <- reactive({
    master <- input$master
    sparkVersion <- input$sparkversion
    hadoopVersion <- input$hadoopversion
  }) %>% debounce(200)

  observe({
    installLater()

    isolate({
      master <- input$master
      sparkVersion <- input$sparkversion
      hadoopVersion <- input$hadoopversion

      checkUserInstallPreference(master, sparkVersion, hadoopVersion, TRUE)
    })
  })

  observe({
    rsApiUpdateDialog(codeReactive())
  })

  observe({
    if (identical(input$master, "cluster")) {
      if (identical(rsApiVersionInfo()$mode, "desktop")) {
        rsApiShowDialog(
          "Connect to Spark",
          paste(
            "Connecting with a remote Spark cluster requires ",
            "an RStudio Server instance that is either within the cluster ",
            "or has a high bandwidth connection to the cluster.</p>",
            "<p>Please see the <strong>Using Spark with RStudio</strong> help ",
            "link below for additional details.</p>",
            sep = ""
          )
        )

        updateSelectInput(
          session,
          "master",
          selected = "local"
        )
      }
      else if (identical(spark_home(), NULL)) {
        rsApiShowDialog(
          "Connect to Spark",
          paste(
            "Connecting with a Spark cluster requires that you are on a system ",
            "able to communicate with the cluster in both directions, and ",
            "requires that the SPARK_HOME environment variable refers to a  ",
            "locally installed version of Spark that is configured to ",
            "communicate with the cluster.",
            "<p>Your system doesn't currently have the SPARK_HOME environment ",
            "variable defined. Please contact your system administrator to ",
            "ensure that the server is properly configured to connect with ",
            "the cluster.<p>",
            sep = ""
          )
        )

        updateSelectInput(
          session,
          "master",
          selected = "local"
        )
      }
      else {
        master <- rsApiShowPrompt(
          "Connect to Cluster",
          "Spark master:",
          "spark://local:7077"
        )

        updateSelectInput(
          session,
          "master",
          choices = c(
            list(master = "master"),
            master,
            spark_ui_default_connections(),
            list("Cluster..." = "cluster")
          ),
          selected = master
        )
      }
    }
  })

  currentSparkSelection <- NULL
  session$onFlushed(function() {
    if (!is_java_available()) {
      url <- ""

      message <- paste(
        "In order to connect to Spark ",
        "your system needs to have Java installed (",
        "no version of Java was detected or installation ",
        "is invalid).",
        sep = ""
      )

      if (identical(rsApiVersionInfo()$mode, "desktop")) {
        message <- paste(
          message,
          "<p>Please contact your server administrator to request the ",
          "installation of Java on this system.</p>",
          sep = "")

        url <- java_install_url()
      } else {
        message <- paste(
          message,
          "<p>Please contact your server administrator to request the ",
          "installation of Java on this system.</p>",
          sep = "")
      }

      rsApiShowDialog(
        "Java Required for Spark Connections",
        message,
        url
      )
    }

    currentSparkSelection <<- spark_default_version()$spark
  })

  observe({
    # Scope this reactive to only changes to spark version
    sparkVersion <- input$sparkversion
    master <- input$master

    # Don't change anything while initializing
    if (!identical(currentSparkSelection, NULL)) {
      currentSparkSelection <<- sparkVersion

      hadoopDefault <- spark_install_find(version = currentSparkSelection, installed_only = FALSE)$hadoopVersion

      updateSelectInput(
        session,
        "hadoopversion",
        choices = spark_ui_hadoop_choices(currentSparkSelection),
        selected = hadoopDefault
      )

      stateValuesReactive$codeInvalidated <<- isolate({
        stateValuesReactive$codeInvalidated + 1
      })
    }
  })

  observe({
    rsApiWritePreference("sparklyr_dbinterface", input$dbinterface)
  })

  outputOptions(output, "notShowVersionsUi", suspendWhenHidden = FALSE)
}

shinyApp(connection_spark_ui, connection_spark_server)
