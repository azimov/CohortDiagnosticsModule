#' @export
BaseJobContext <- R6::R6Class(
  "BaseJobContext",
  private = list(
    keyring = NULL,
    rawSpecification = NULL,
    useSecurePassword = NULL,
    keyingUser = NULL,
    keyringPasswordStore = NULL,
    #' Returns value from keyring
    getDbPassword = function() {
        return(keyring::key_get(private$keyringPasswordStore,
                                username = private$keyringUser,
                                keyring = private$keyring))
    }
  ),

  public = list(
    cdmDatabase = NULL,
    executionSettings = NULL,
    analysisSpecification = NULL,
    initialize = function(jsonSpecificationPath,
                          cdmDatabase,
                          useSecurePassword = TRUE,
                          keyringPasswordStore = NULL,
                          keyringUser = NULL,
                          keyring = NULL) {
      checkmate::assert_file_exists(jsonSpecificationPath)
      private$rawSpecification <- readChar(jsonSpecificationPath, file.info(jsonSpecificationPath)$size)
      contextOptions <- jsonlite::fromJSON(txt = private$rawSpecification)

      self$executionSettings <- contextOptions$executionSettings
      self$analysisSpecification <- contextOptions$analysisSpecification
      private$keyring <- keyring
      private$keyringPasswordStore <- keyringPasswordStore
      private$keyingUser <- keyringUser
      private$useSecurePassword <- useSecurePassword
      self$cdmDatabase <- cdmDatabase

      if (useSecurePassword & !is.null(self$cdmDatabase$connectionDetails$password)) {
        stop("For secure storage, password should be stored in keyring store. Use keyring::set_key to set")
      }
    },

        #' Loads cohorts from json specification
        #' Subclass and override to load from a package or, for example, a WebApi service
    getCohortDefinitionSet = function() {
      if (length(self$analysisSpecification) <= 0) {
        stop("No cohort definitions found")
      }
      cohortDefinitionSet <- data.frame()
      for (i in self$analysisSpecification$cohortDefinitions$cohortId) {
        cohortDef <- self$analysisSpecification$cohortDefinitions %>%
          dplyr::filter(.data$cohortId == i)

        cohortJson <- cohortDef %>%
          dplyr::select(.data$cohortDefinition) %>%
          dplyr::pull()
        cohortName <- cohortDef %>%
          dplyr::select(.data$cohortName) %>%
          dplyr::pull()
        cohortExpression <- CirceR::cohortExpressionFromJson(cohortJson)
        cohortSql <- CirceR::buildCohortQuery(cohortExpression, options = CirceR::createGenerateOptions(generateStats = TRUE))
        cohortDefinitionSet <- rbind(cohortDefinitionSet, data.frame(cohortId = i,
                                                                     cohortName = cohortName,
                                                                     sql = cohortSql,
                                                                     json = cohortJson,
                                                                     logicDescription = cohortName,
                                                                     stringsAsFactors = FALSE))
      }
      return(cohortDefinitionSet)
    },

    #' Returns DatabaseConnector connectionDetails object using secured credentials
    getConnectionDetails = function() {
      connectionDetails <- self$cdmDatabase$connectionDetails
      if (private$useSecurePassword) {
        connectionDetails <- c(connectionDetails, list(password = private$getDbPassword()))
      }
      do.call(DatabaseConnector::createConnectionDetails, connectionDetails)
    },

    getExecutionParameters = function(...) {
      stop("Function should be implemented by subclass")
    },
    validateSettings = function() {
      # Connect to database
      ParallelLogger::logInfo("Testing database connection")
      connection <- DatabaseConnector::connect(self$getConnectionDetails())
      DatabaseConnector::disconnect(connection)
      ParallelLogger::logInfo("Database connection settings are valid")
      # Check CdmDatabase schema etc are set
      checkmate::assertString(self$cdmDatabase$cdmDatabaseSchema)
    }
  )
)

#' Subclass for implementation specific settings
#' @export
CDJobContext <- R6::R6Class(
  "CDJobContext",
  inherit = BaseJobContext,
  public = list(
    validateSettings = function() {
      super$validateSettings()
      # Check that specific settings are correct
      checkmate::assertString(self$cdmDatabase$databaseId)
      checkmate::assertString(self$cdmDatabase$workDatabaseSchema)

      # A better pattern would be to expose the parameter checks in cohort diagnostics directly
      checkmate::assertLogical(self$executionSettings$runInclusionStatistics)
      checkmate::assertLogical(self$executionSettings$runIncludedSourceConcepts)
      checkmate::assertLogical(self$executionSettings$runOrphanConcepts)
      checkmate::assertLogical(self$executionSettings$runTimeDistributions)
      checkmate::assertLogical(self$executionSettings$runVisitContext)
      checkmate::assertLogical(self$executionSettings$runBreakdownIndexEvents)
      checkmate::assertLogical(self$executionSettings$runIncidenceRate)
      checkmate::assertLogical(self$executionSettings$runCohortOverlap)
      checkmate::assertLogical(self$executionSettings$runCohortCharacterization)
      checkmate::assertLogical(self$executionSettings$runTemporalCohortCharacterization)
      checkmate::assertInt(self$executionSettings$minCellCount, lower = 1)
      checkmate::assertList(self$executionSettings$covariateSettings, null.ok = TRUE)
      checkmate::assertList(self$executionSettings$temporalCovariateSettings)

    },
    getExecutionParameters = function(...) {
      covariateSettings <- do.call(FeatureExtraction::createDefaultCovariateSettings,
                                   as.list(self$executionSettings$covariateSettings))

      temporalCovariateSettings <- do.call(FeatureExtraction::createTemporalCovariateSettings,
                                           self$executionSettings$temporalCovariateSettings)

      list(connectionDetails = self$getConnectionDetails(),
           cohortDefinitionSet = self$getCohortDefinitionSet(),
           cdmDatabaseSchema = self$cdmDatabase$cdmDatabaseSchema,
           cohortDatabaseSchema = self$cdmDatabase$workDatabaseSchema,
           cohortTableNames = self$executionSettings$cohortTableNames,
           incremental = self$analysisSpecification$incremental,
           databaseId = self$cdmDatabase$databaseId,
           incrementalFolder = file.path(self$executionSettings$exportFolder, "incremental"),
           exportFolder = normalizePath(self$executionSettings$exportFolder),
           runInclusionStatistics = self$executionSettings$runInclusionStatistics,
           runIncludedSourceConcepts = self$executionSettings$runIncludedSourceConcepts,
           runOrphanConcepts = self$executionSettings$runOrphanConcepts,
           runTimeDistributions = self$executionSettings$runTimeDistributions,
           runVisitContext = self$executionSettings$runVisitContext,
           runBreakdownIndexEvents = self$executionSettings$runBreakdownIndexEvents,
           runIncidenceRate = self$executionSettings$runIncidenceRate,
           runCohortOverlap = self$executionSettings$runCohortOverlap,
           runCohortCharacterization = self$executionSettings$runCohortCharacterization,
           covariateSettings = covariateSettings,
           runTemporalCohortCharacterization = self$executionSettings$runTemporalCohortCharacterization,
           temporalCovariateSettings = temporalCovariateSettings,
           minCellCount = self$executionSettings$minCellCount)
    }
  )
)
