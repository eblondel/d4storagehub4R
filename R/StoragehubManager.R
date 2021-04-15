#' StoragehubManager
#' @docType class
#' @export
#' @keywords storagehub manager
#' @return Object of \code{\link{R6Class}} for modelling an StoragehubManager
#' @format \code{\link{R6Class}} object.
#' @section Methods:
#' \describe{
#'  \item{\code{new(url, token, logger)}}{
#'    This method is used to instantiate the \code{StoragehubManager}. By default,
#'    the url is inherited through D4Science Icproxy service.
#'    
#'    The token is mandatory in order to use Storage HUb API.
#'    
#'    The logger can be either NULL, "INFO" (with minimum logs), or "DEBUG" 
#'    (for complete curl http calls logs)
#'  }
#'  \item{\code{getToken()}}{
#'    Get user token.
#'  }
#'  \item{\code{getUserProfile()}}{
#'    Get user profile.
#'  }
#'  \item{\code{getUserWorkspace()}}{
#'    Get user workspace root path.
#'  }
#'  \item{\code{getWSRootID()}}{
#'    Get workspace root ID
#'  }
#'  \item{\code{getWSElementID(parentFolderID, folderPath)}}{
#'    Get workspace element ID
#'  }
#'  \item{\code{listWSElements(parentFolderID)}}{
#'    Lists workspace elements given a parentFolder ID
#'  }
#'  \item{\code{listWSElementsByPath(folderPath)}}{
#'    Lists workspace elements given a folder path
#'  }
#'  \item{\code{searchWSFolderID(folderPath)}}{
#'    Searchs a folder ID based on folder path
#'  }
#'  \item{\code{createFolder(folderPath, name, description, hidden)}}{
#'    Creates a folder
#'  }
#'  \item{\code{uploadFile(folderPath, file, description, archive)}}{
#'    Uploads a file
#'  }
#' }
#' 
#' @examples
#' \dontrun{
#' }
#' 
#' @note Main user class to be used with \pkg{d4storagehub4R}
#' 
#' @author Emmanuel Blondel <emmanuel.blondel1@@gmail.com>
#' 
StoragehubManager <-  R6Class("StoragehubManager",
  inherit = d4storagehub4RLogger,
  private = list(
    keyring_service = NULL,
    user_profile = NULL,
    user_workspace = NULL,
    url_icproxy = "https://registry.d4science.org/icproxy/gcube/service/GCoreEndpoint/DataAccess/StorageHub",
    url_homelibrary = "https://socialnetworking1.d4science.org/social-networking-library-ws/rest/2",
    url_storagehub = NULL,
    
    #utils
    #normalizeFolderPath
    normalizeFolderPath = function(path){
      if(endsWith(path, "/")) path = substr(path,0,nchar(path)-1)
      return(path)
    }
  ),
  public = list(
    #logger
    verbose.info = FALSE,
    verbose.debug = FALSE,
    loggerType = NULL,
    logger = function(type, text){
      if(self$verbose.info){
        cat(sprintf("[d4storagehub4R][%s] %s - %s \n", type, self$getClassName(), text))
      }
    },
    INFO = function(text){self$logger("INFO", text)},
    WARN = function(text){self$logger("WARN", text)},
    ERROR = function(text){self$logger("ERROR", text)},
    
    initialize = function(token, logger = NULL){
      super$initialize(logger = logger)
      if(!is.null(token)) if(nzchar(token)){
        private$keyring_service = paste0("d4storagehub4R@", private$url_icproxy)
        keyring::key_set_with_value(private$keyring_service, username = "d4storagehub4R", password = token)
        self$fetchWSEndpoint()
        self$fetchUserProfile()
      }else{
        self$INFO("Successfully connected to Storage hub as anonymous user")
      }
    },
    
    #getToken
    getToken = function(){
      token <- NULL
      if(!is.null(private$keyring_service)){
        token <- suppressWarnings(keyring::key_get(private$keyring_service, username = "d4storagehub4R"))
      }
      return(token)
    },
    
    #getUserProfile
    getUserProfile = function(){
      return(private$user_profile)
    },
    
    #getUserWorkspace
    getUserWorkspace = function(){
      return(private$user_workspace)
    },
    
    #fetchWSEndpoint
    fetchWSEndpoint = function(){
      self$INFO("Fetching workspace endpoint...")
      icproxy = paste0(private$url_icproxy, "?gcube-token=", self$getToken())
      icproxy_req <- httr::GET(icproxy)
      httr::stop_for_status(icproxy_req)
      xml = XML::xmlParse(httr::content(icproxy_req), "text")
      private$url_storagehub = XML::xpathSApply(xml, "//Endpoint", xmlValue)[1]
    },
    
    #fetchUserProfile
    fetchUserProfile = function(){
      self$INFO("Fetching user profile...")
      user_profile_url = paste0(private$url_homelibrary, "/people/profile?gcube-token=", self$getToken())
      user_profile_req <- httr::GET(user_profile_url)
      httr::stop_for_status(user_profile_req)
      user_profile = httr::content(user_profile_req)
      private$user_profile = user_profile$result
      private$user_workspace = paste0("/Home/", private$user_profile$username, "/Workspace")
    },
    
    #getWSRootID
    getWSRootID = function(){
      rootUrl <- paste0(private$url_storagehub, "?exclude=hl:accounting&gcube-token=", self$getToken())
      rootDoc <- jsonlite::fromJSON(rootUrl)
      return(rootDoc$item$id)
    },
    
    #getWSElementID
    getWSElementID = function(parentFolderID = NULL, folderPath){
      elements <- self$listWSElements(parentFolderID = parentFolderID)
      
      wsFolderID <- NULL
      for (i in 1:nrow(elements)){
        el <- elements[i,]
        if (!startsWith(el$path,"/Share/")){
          el_path = el$path
          if(startsWith(el_path, self$getUserWorkspace())){
            el_path <- unlist(strsplit(el_path, paste0(self$getUserWorkspace(),"/")))[2]
          }
          if(startsWith(folderPath, self$getUserWorkspace())){
            folderPath = unlist(strsplit(folderPath, paste0(self$getUserWorkspace(),"/")))[2]
          }
          if (folderPath == el_path || folderPath == paste0("/",el$path)){
            wsFolderID = el$id
            break
          }
        }else{
          path.parts = unlist(strsplit(folderPath,"/"))
          folder <- path.parts[length(path.parts)]
          el_ws_parts = unlist(strsplit(el$path, "/"))
          el_ws_folder = el_ws_parts[length(el_ws_parts)]
          if (folder == el_ws_folder){
            wsFolderID<-el$id
            break
          }
        }
      }
      return(wsFolderID)
      
    },
    
    #listWSElements
    listWSElements = function(parentFolderID = NULL){
      if(is.null(parentFolderID)) parentFolderID = self$getWSRootID()
      listElementsUrl = paste0(private$url_storagehub, "/items/", parentFolderID, "/children?exclude=hl:accounting&gcube-token=", self$getToken())
      out = jsonlite::fromJSON(listElementsUrl)
      return(out$itemlist)
    },
    
    #listWSElementsByPath
    listWSElementsByPath = function(folderPath){
      folderID <- self$searchWSFolderID(folderPath = folderPath)
      if(is.null(folderID)) return(NULL)
      self$listWSElements(parentFolderID = folderID)
    },
    
    #searchWSFolderID
    searchWSFolderID = function(folderPath){
      rootID = self$getWSRootID()
      root <- self$getUserWorkspace()
      
      if (folderPath==paste("/Home/",self$getUserProfile()$username,"/Workspace",sep="") || 
          folderPath==paste("/Home/",self$getUserProfile()$username,"/Workspace/",sep="")){
        return(rootID)
      }
      
      path.splits <- unlist(strsplit(folderPath, "Workspace"))
      if(length(path.splits)>1) folderPath <- path.splits[2]
      allsubfolders = unlist(strsplit(folderPath, "/"))
      allsubfolders = allsubfolders[nzchar(allsubfolders)]
      
      parentID = rootID
      parentPath = root
      for (subfolder in allsubfolders){
        parentPath = paste0(parentPath,"/",subfolder)
        parentID = self$getWSElementID(parentFolderID = parentID, folderPath = parentPath)
      }
      return(parentID)
    },
    
    #createFolder
    createFolder = function(folderPath = NULL, name, description = "", hidden = FALSE){
      self$INFO(sprintf("Creating folder '%s at '%s'...", name, folderPath))
      if(is.null(folderPath)) folderPath = self$getUserWorkspace()
      pathID = self$searchWSFolderID(folderPath = folderPath)
      if(is.null(pathID)){
        errMsg <- sprintf("No folder for path '%s'", folderPath)
        self$ERROR(errMsg)
        stop(errMsg)
      }
      
      req <- NULL
      if(!self$verbose.debug){
        req = httr::POST(
          paste0(private$url_storagehub, "/items/",pathID,'/create/FOLDER?gcube-token=', self$getToken()),
          body = list(
            name = name,
            description = description,
            hidden = hidden
          ),
          encode = "form"
        )
      }else{
        req <- httr::with_verbose(
          httr::POST(
            paste0(private$url_storagehub, "/items/",pathID,'/create/FOLDER?gcube-token=', self$getToken()),
            body = list(
              name = name,
              description = description,
              hidden = hidden
            ),
            encode = "form"
          )
        )
      }
      stop_for_status(req)
      folderID <- content(req, "text")
      return(folderID)
    },
    
    #uploadFile
    uploadFile = function(folderPath = NULL, file, description = basename(file), archive = FALSE){
      self$INFO(sprintf("Uploading file '%s' at '%s'...", file, folderPath))
      if(is.null(folderPath)) folderPath = self$getUserWorkspace()
      
      name = basename(file)
      folderPath <- private$normalizeFolderPath(folderPath)
      pathID <- self$searchWSFolderID(folderPath = folderPath)
      if(is.null(pathID)){
        errMsg <- sprintf("No folder for path '%s'", folderPath)
        self$ERROR(errMsg)
        stop(errMsg)
      }
      
      absolutefile <- tools:::file_path_as_absolute(file)
      wdfile <- file.path(getwd(), basename(file))
      localfile <- absolutefile
      
      type <- ifelse(archive, "ARCHIVE", "FILE")
      
      upload_url <- sprintf("%s/items/%s/create/%s?gcube-token=%s", private$url_storagehub, pathID, type, self$getToken())
      
      upload_req <- NULL
      if(!self$verbose.debug){
        upload_req <- httr::POST(
          url = upload_url,
          body = list(
            name = name,
            description = description,
            file = httr::upload_file(file)
          )
        )
      }else{
        upload_req <- httr::with_verbose(
          httr::POST(
            url = upload_url,
            body = list(
              name = name,
              description = description,
              file = httr::upload_file(file)
            )
          )
        )
      }
      fileID <- NULL
      if(httr::status_code(upload_req)==200){
        fileID <- httr::content(upload_req, "text")
        self$INFO("Successful upload to workspace!")
      }else{
        errMsg <- sprintf("Error while trying to upload file '%s' to '%s'", file, upload_url)
        self$ERROR(errMsg)
        stop(errMsg)
      }
      
      return(fileID)
    }
    
  )
)
