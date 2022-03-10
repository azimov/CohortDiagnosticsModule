createCohortDefinitionSetFromJobContext <- function(analysisSpecification) {
  if (length(analysisSpecification) <= 0) {
    stop("No cohort definitions found")
  }
  cohortDefinitionSet <- data.frame()
  for (i in analysisSpecification$cohortDefinitions$cohortId) {
    cohortDef <- analysisSpecification$cohortDefinitions %>%
      dplyr::filter(.data$cohortId == i)

    cohortJson <- cohortDef %>% dplyr::select(.data$cohortDefinition) %>% dplyr::pull()
    cohortName <- cohortDef %>% dplyr::select(.data$cohortName) %>% dplyr::pull()
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
}

logStatus <- function(status) {
  message <- paste0(status, " - ", utils::packageName(), " (v", utils::packageVersion(pkg = utils::packageName()), ")")
  ParallelLogger::logInfo(paste0(rep("-", nchar(message))))
  ParallelLogger::logInfo(message)
  ParallelLogger::logInfo(paste0(rep("-", nchar(message))))
}

getResultsFolderName <- function(executionSettings) {
  return(file.path(getOutputFolder(executionSettings), "results"))
}

getIncrementalFolderName <- function(executionSettings) {
  return(file.path(getOutputFolder(executionSettings), "incremental"))
}

getOutputFolder <- function(executionSettings) {
  return(file.path(executionSettings$outputFolder, utils::packageName()))
}
