#' d4storagehub4RLogger
#'
#' @docType class
#' @export
#' @keywords logger
#' @return Object of \code{\link{R6Class}} for modelling a simple logger
#' @format \code{\link{R6Class}} object.
#'
#' @section Abstract Methods:
#' \describe{
#'  \item{\code{INFO(text)}}{
#'    Logger to report information. Used internally
#'  }
#'  \item{\code{WARN(text)}}{
#'    Logger to report warnings. Used internally
#'  }
#'  \item{\code{ERROR(text)}}{
#'    Logger to report errors. Used internally
#'  }
#' }
#' 
#' @note Logger class used internally by d4storagehub4R
#'
d4storagehub4RLogger <-  R6Class("d4storagehub4RLogger",
  public = list(
    #'@field verbose.info verbose info, default is \code{FALSE}
    verbose.info = FALSE,
    #'@field verbose.debug verbose debug, default is \code{FALSE}
    verbose.debug = FALSE,
    #'@field loggerType logger type
    loggerType = NULL,
    
    #'@description Logger function
    #'@param type type
    #'@param text text
    logger = function(type, text){
      if(self$verbose.info){
        cat(sprintf("[d4storagehub4R][%s] %s - %s \n", type, self$getClassName(), text))
      }
    },
    
    #'@description INFO logger function
    #'@param text text
    INFO = function(text){self$logger("INFO", text)},
    #'@description WARN logger function
    #'@param text text
    WARN = function(text){self$logger("WARN", text)},
    #'@description ERROR logger function
    #'@param text text
    ERROR = function(text){self$logger("ERROR", text)},
    
    #'@description Initializes a basic logger class
    #'@param logger the type of logger, either \code{NULL} (default), \code{INFO}, or \code{DEBUG}
    initialize = function(logger = NULL){
      
      #logger
      if(!missing(logger)){
        if(!is.null(logger)){
          self$loggerType <- toupper(logger)
          if(!(self$loggerType %in% c("INFO","DEBUG"))){
            stop(sprintf("Unknown logger type '%s", logger))
          }
          if(self$loggerType == "INFO"){
            self$verbose.info = TRUE
          }else if(self$loggerType == "DEBUG"){
            self$verbose.info = TRUE
            self$verbose.debug = TRUE
          }
        }
      }
    },
    
    #'@description Get class name
    #'@return the class name
    getClassName = function(){
      return(class(self)[1])
    },
    
    #'@description Get class
    #'@return the class
    getClass = function(){
      class <- eval(parse(text=self$getClassName()))
      return(class)
    }
    
  )
)