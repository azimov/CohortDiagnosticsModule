# Copyright 2022 Observational Health Data Sciences and Informatics
#
# This file is part of CohortDiagnosticsModule
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#' @export
validate <- function(jobContext) {
  logStatus("Validate")
  # Verify the job context details - this feels like a task to centralize for
  # all modules
  jobContext$validateSettings()

  # Validate that the analysis specification will work when we
  # enter the execute statement. Bad thing here: we're doing
  # double work to construct the cohort definition set but I'm
  # unsure if validate() should potentially change the jobContext
  # to add any necessary elements to the executionSettings list?
  cohortDefinitionSet <- jobContext$getCohortDefinitionSet()
  executionSettings <- jobContext$getExecutionParameters()
  invisible(cohortDefinitionSet)
}

#' @export
execute <- function(jobContext) {
  logStatus("Execute")
  # Create the cohort definition set
  cohortDefinitionSet <- jobContext$getCohortDefinitionSet()
  executionSettings <- jobContext$getExecutionParameters()
  # run diagnostics
  do.call(CohortDiagnostics::executeDiagnostics, executionSettings)
  return(TRUE)
}

#' @export
exportResults <- function(jobContext) {
  logStatus("Export data")
  # Establish the connection and ensure the cleanup is performed
  connection <- DatabaseConnector::connect(jobContext$cdmDatabase$connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  executionSettings <- jobContext$getExecutionParameters()
  # Create Sqlite Db for shiny app
  CohortDiagnostics::createMergedResultsFile(executionSettings$exportFolder, overwrite = TRUE)
}

#' @export
importResults <- function(jobContext) {
  return(NULL)
}
