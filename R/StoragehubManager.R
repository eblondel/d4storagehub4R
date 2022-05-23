#' StoragehubManager
#' @docType class
#' @export
#' @keywords storagehub manager
#' @return Object of \code{\link{R6Class}} for modelling a D4Science StoragehubManager
#' 
#' @examples
#' \dontrun{
#'   manager <- StoragehubManager$new(
#'     token = "<your token>",
#'     logger = "DEBUG"
#'   )
#' }
#' 
#' @note Main user class to be used with \pkg{d4storagehub4R}
#' 
#' @author Emmanuel Blondel <emmanuel.blondel1@@gmail.com>
#' 
StoragehubManager <-  R6Class("StoragehubManager",
  inherit = d4storagehub4RLogger,
  private = list(
    keyring_backend = NULL,
    keyring_service = NULL,
    user_profile = NULL,
    user_workspace = NULL,
    url_icproxy = "https://registry.d4science.org/icproxy/gcube/service/GCoreEndpoint/DataAccess/StorageHub",
    url_homelibrary = "https://api.d4science.org/rest/2",
    url_storagehub = "https://api.d4science.org/workspace",
    token_type = NULL,
    supportedTokenTypes = c("gcube", "jwt"),
    #utils
    #normalizeFolderPath
    normalizeFolderPath = function(path){
      if(endsWith(path, "/")) path = substr(path,0,nchar(path)-1)
      return(path)
    }
  ),
  public = list(
    
    #'@description Method is used to instantiate the \link{StoragehubManager}.
    #'@param token user access token
    #'@param token_type token type, either 'gcube' (default) or 'jwt'
    #'@param logger logger can be either NULL, "INFO" (with minimum logs), or "DEBUG" (for complete 
    #' curl http calls logs)
    #'@param keyring_backend keyring backend to use.it can be set to use a different backend for storing 
    #' the D4science gcube token with \pkg{keyring} (Default value is 'env').
    initialize = function(token, token_type = 'gcube', logger = NULL, keyring_backend = 'env'){
      super$initialize(logger = logger)
      if(!is.null(token)) if(nzchar(token)){
        if(!keyring_backend %in% names(keyring:::known_backends)){
          errMsg <- sprintf("Backend '%s' is not a known keyring backend!", keyring_backend)
          self$ERROR(errMsg)
          stop(errMsg)
        }
        private$token_type <- token_type
        private$keyring_backend <- keyring:::known_backends[[keyring_backend]]$new()
        private$keyring_service = paste0("d4storagehub4R@", private$url_storagehub)
        private$keyring_backend$set_with_value(service = private$keyring_service, username = "d4storagehub4R", password = token)
        self$fetchUserProfile()
      }else{
        self$INFO("Successfully connected to Storage hub as anonymous user")
      }
    },
    
    #'@description Get token
    #'@return the user access token
    getToken = function(){
      token <- NULL
      if(!is.null(private$keyring_service)){
        token <- suppressWarnings(private$keyring_backend$get(service = private$keyring_service, username = "d4storagehub4R"))
      }else{
        token <- private$token
      }
      return(token)
    },
    
    #'@description Get user profile
    #'@return the user profile
    getUserProfile = function(){
      return(private$user_profile)
    },
    
    #'@description Get user workspace
    #'@return the user workspace root path
    getUserWorkspace = function(){
      return(private$user_workspace)
    },
    
    #'@description Fetches the workspace endpoint from the D4Science ICProxy service
    #'@note Deprecated
    fetchWSEndpoint = function(){
      self$INFO("Fetching workspace endpoint...")
      icproxy_req <- switch(private$token_type,
        "gcube" = {
          icproxy = paste0(private$url_icproxy, "?gcube-token=", self$getToken())
          httr::GET(icproxy)
        },
        "jwt" = {
          httr::GET(private$url_icproxy, httr::add_headers("Authorization" = paste("Bearer", self$getToken())))
        }
      )
      httr::stop_for_status(icproxy_req)
      if(!is.null(icproxy_req)){
        xml = XML::xmlParse(httr::content(icproxy_req), "text")
        private$url_storagehub = XML::xpathSApply(xml, "//Endpoint", xmlValue)[1]
      }
    },
    
    #'@description Fetches the user profile
    fetchUserProfile = function(){
      self$INFO("Fetching user profile...")
      user_profile_req <- switch(private$token_type,
        "gcube" = {
          user_profile_url = paste0(private$url_homelibrary, "/people/profile?gcube-token=", self$getToken())
          httr::GET(user_profile_url)
        },
        "jwt" = {
          user_profile_url = paste0(private$url_homelibrary, "/people/profile")
          httr::with_verbose(httr::GET(user_profile_url, httr::add_headers("Authorization" = paste("Bearer", self$getToken()))))
        }
      )
      
      httr::stop_for_status(user_profile_req)
      if(!is.null(user_profile_req)){
        user_profile = httr::content(user_profile_req)
        private$user_profile = user_profile$result
        private$user_workspace = paste0("/Home/", private$user_profile$username, "/Workspace")
      }
    },
    
    #'@description Get workspace root ID
    #'@return the workspace root ID, as \code{character}
    getWSRootID = function(){
      outroot <- NULL
      root_req <- switch(private$token_type,
        "gcube" = {
          rootUrl <- paste0(private$url_storagehub, "?exclude=hl:accounting&gcube-token=", self$getToken())
          httr::GET(rootUrl)
        },
        "jwt" = {
          rootUrl <- paste0(private$url_storagehub, "?exclude=hl:accounting")
          httr::GET(rootUrl, httr::add_headers("Authorization" = paste("Bearer", self$getToken())))
        }
      )
      if(!is.null(root_req)){
        rootDoc <- httr::content(root_req)
        outroot <- rootDoc$item$id
      }
      return(outroot)
    },
    
    #'@description Get workspace item ID given a \code{folderPath} in a parent folder
    #'@param parentFolderID parent folder ID
    #'@param folderPath folder path
    #'@return the workspace item ID, \code{NULL} if no workspace item existing
    getWSItemID = function(parentFolderID = NULL, folderPath){
      elements <- self$listWSItems(parentFolderID = parentFolderID)
      
      wsItemID <- NULL
      if(length(elements)>0) for (i in 1:nrow(elements)){
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
            wsItemID = el$id
            break
          }
        }else{
          path.parts = unlist(strsplit(folderPath,"/"))
          folder <- path.parts[length(path.parts)]
          el_ws_parts = unlist(strsplit(el$path, "/"))
          el_ws_folder = el_ws_parts[length(el_ws_parts)]
          if (folder == el_ws_folder){
            wsItemID<-el$id
            break
          }
        }
      }
      return(wsItemID)
      
    },
    
    #'@description Get workspace element ID given a \code{folderPath} in a parent folder
    #'@param parentFolderID parent folder ID
    #'@param folderPath folder path
    #'@return the workspace element ID, \code{NULL} if no workspace item existing
    #'@note Deprecated, use \code{getWSItemID}
    getWSElementID = function(parentFolderID = NULL, folderPath){
      self$WARN("Method 'getWSElementID' is deprecated, use method 'getWSItemID' instead!")
      self$getWSItemID(parentFolderID = parentFolderID, folderPath = folderPath)
    },
    
    #'@description Lists workspace items given a parentFolder ID
    #'@param parentFolderID parent folder ID
    #'@return an object of class \code{data.frame}
    listWSItems = function(parentFolderID = NULL){
      outlist <- NULL
      if(is.null(parentFolderID)) parentFolderID = self$getWSRootID()
      listElementsUrl = paste0(private$url_storagehub, "/items/", parentFolderID, "/children?exclude=hl:accounting")
      list_req <- switch(private$token_type,
        "gcube" = {
          listElementsUrl <- paste0(listElementsUrl, "&gcube-token=", self$getToken())
          httr::GET(listElementsUrl)
        },
        "jwt" = {
          httr::GET(listElementsUrl, httr::add_headers("Authorization" = paste("Bearer", self$getToken())))
        }
      )
      if(!is.null(list_req)){
        out = jsonlite:::simplify(httr::content(list_req))
        outlist <- out$itemlist
      }
      return(outlist)
    },
    
    #'@description Lists workspace elements given a parentFolder ID
    #'@param parentFolderID parent folder ID
    #'@return an object of class \code{data.frame}
    #'@note Deprecated, use \code{listWSItems}
    listWSElements = function(parentFolderID = NULL){
      self$WARN("Method 'listWSElements' is deprecated, use method 'listWSItems' instead!")
      self$listWSItems(parentFolderID = parentFolderID)
    },
    
    #'@description Lists workspace items given a folder path
    #'@param folderPath folder path where to list items
    #'@return an object of class \code{data.frame}
    listWSItemsByPath = function(folderPath){
      folderID <- self$searchWSItemID(itemPath = folderPath)
      if(is.null(folderID)) return(NULL)
      self$listWSItems(parentFolderID = folderID)
    },
    
    #'@description Lists workspace elements given a folder path
    #'@param folderPath folder path where to list elements
    #'@return an object of class \code{data.frame}
    #'@note Deprecated, use \code{listWSItemsByPath}
    listWSElementsByPath = function(folderPath){
      self$WARN("Method 'listWSElementsByPath' is deprecated, use method 'listWSItemsByPath' instead!")
      self$listWSItemsByPath(folderPath = folderPath)
    },
    
    #'@description Searches for a workspace item ID given a item path
    #'@param itemPath path of the item
    #'@return the item ID, \code{NULL} if nothing found
    searchWSItemID = function(itemPath){
      rootID = self$getWSRootID()
      root <- self$getUserWorkspace()
      
      if (itemPath==paste("/Home/",self$getUserProfile()$username,"/Workspace",sep="") || 
          itemPath==paste("/Home/",self$getUserProfile()$username,"/Workspace/",sep="")){
        return(rootID)
      }
      
      path.splits <- unlist(strsplit(itemPath, "Workspace"))
      if(length(path.splits)>1) itemPath <- path.splits[2]
      allsubfolders = unlist(strsplit(itemPath, "/"))
      allsubfolders = allsubfolders[nzchar(allsubfolders)]
      
      parentID = rootID
      parentPath = root
      for (subfolder in allsubfolders){
        parentPath = paste0(parentPath,"/",subfolder)
        parentID = self$getWSItemID(parentFolderID = parentID, folderPath = parentPath)
      }
      return(parentID)
    },
    
    #'@description Searches for a workspace element ID given a item path
    #'@param folderPath path of the element
    #'@return the item ID, \code{NULL} if nothing found
    #'@note Deprecated, use \code{searchWSItemID}
    searchWSFolderID = function(folderPath){
      self$WARN("Method 'searchWSFolderID' is deprecated, use method 'searchWSItemID' instead!")
      return(self$searchWSItemID(itemPath = folderPath))
    },
    
    #'@description Creates a folder, given a folder path, a folder name/description. By default \code{recursive = TRUE} meaning 
    #'    that a folder path matching nested folders will trigger all nested folders. Setting \code{recursive = FALSE}, the
    #'    folder creation will work only if the folder path matches an existing folder. The \code{hidden} (default 
    #'    \code{FALSE}) argument can be used to set hidden folders on the workspace.
    #'@param folderPath parent folder path where to create the folder
    #'@param name name of the folder
    #'@param description description of the folder
    #'@param hidden hidden, default is \code{FALSE}
    #'@param recursive recursive, default is \code{TRUE}
    #'@return the ID of the created folder
    createFolder = function(folderPath = NULL, name, description = "", 
                            hidden = FALSE, recursive = TRUE){
      self$INFO(sprintf("Creating folder '%s at '%s'...", name, folderPath))
      if(is.null(folderPath)) folderPath = self$getUserWorkspace()
      
      
      if(recursive){
        self$INFO("Recursive mode - Check parent folder(s) and create them if missing...")
        folder_paths <- data.frame(folderPath = character(0), name = character(0), stringsAsFactors = FALSE)
        if(folderPath == self$getUserWorkspace()){
          folder_paths <- data.frame(
            folderPath = folderPath,
            name = name,
            stringsAsFactors = FALSE
          )
        }else{
          parent_folder <- folderPath
          while(parent_folder != "."){
            
            folder_path <- data.frame(
              folderPath = dirname(parent_folder),
              name = basename(parent_folder),
              stringsAsFactors = FALSE
            )
            parent_folder <- folder_path$folderPath
            folder_paths <- rbind(folder_paths, folder_path)
          }
          folder_paths <- folder_paths[order(row.names(folder_paths), decreasing = T),]
          folder_paths <- rbind(folder_paths,
                                data.frame(
                                  folderPath = folderPath,
                                  name = name,
                                  stringsAsFactors = FALSE
                                ))
          folder_paths[folder_paths$folderPath == ".",]$folderPath <- self$getUserWorkspace()
        }
        folderID <- NULL
        for(i in 1:nrow(folder_paths)){
          folder_path <- folder_paths[i,]
          self$INFO(sprintf("Search for an existing folder '%s'", file.path(folder_path$folderPath, folder_path$name)))
          folderID <- self$searchWSItemID(itemPath = file.path(folder_path$folderPath, folder_path$name))
          if(is.null(folderID)){
            self$INFO(sprintf("Folder '%s' does not exist, we create it...", file.path(folder_path$folderPath, folder_path$name)))
            folderID <- self$createFolder(
              folderPath = folder_path$folderPath,
              name = folder_path$name,
              recursive = FALSE
            )
          }else{
            self$WARN(sprintf("Folder '%s' already exist, skip creation...", file.path(folder_path$folderPath, folder_path$name)))
          }
        }
        return(folderID)
        
      }else{
        pathID = self$searchWSItemID(itemPath = folderPath)
        if(is.null(pathID)){
          errMsg <- sprintf("No folder for path '%s'", folderPath)
          self$ERROR(errMsg)
          stop(errMsg)
        }
        req <- switch(private$token_type,
          "gcube" = {
            if(!self$verbose.debug){
              httr::POST(
                paste0(private$url_storagehub, "/items/",pathID,'/create/FOLDER?gcube-token=', self$getToken()),
                body = list(
                  name = name,
                  description = description,
                  hidden = hidden
                ),
                encode = "form"
              )
            }else{
              httr::with_verbose(
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
          },
          "jwt" = {
            if(!self$verbose.debug){
              httr::POST(
                paste0(private$url_storagehub, "/items/",pathID,'/create/FOLDER'),
                body = list(
                  name = name,
                  description = description,
                  hidden = hidden
                ),
                encode = "form",
                httr::add_headers("Authorization" = paste("Bearer", self$getToken()))
              )
            }else{
              httr::with_verbose(
                httr::POST(
                  paste0(private$url_storagehub, "/items/",pathID,'/create/FOLDER'),
                  body = list(
                    name = name,
                    description = description,
                    hidden = hidden
                  ),
                  encode = "form",
                  httr::add_headers("Authorization" = paste("Bearer", self$getToken()))
                )
              )
            }
          }
        )
        httr::stop_for_status(req)
        folderID <- content(req, "text")
        return(folderID)
      }
    },
    
    #'@description  Uploads a file to a folder (given a folder path). The argument \code{description} can be used to further describe the
    #'    file to upload. The argument \code{archive} (default = FALSE) indicates the type of item (FILE or ARCHIVE) to be uploaded.
    #'@param folderPath folder path where to upload the file
    #'@param file file to upload
    #'@param description file description, default would be the file basename
    #'@param archive archive, default is \code{FALSE} 
    #'@return the ID of the uploaded file   
    uploadFile = function(folderPath = NULL, file, description = basename(file), archive = FALSE){
      self$INFO(sprintf("Uploading file '%s' at '%s'...", file, folderPath))
      if(is.null(folderPath)) folderPath = self$getUserWorkspace()
      
      name = basename(file)
      folderPath <- private$normalizeFolderPath(folderPath)
      pathID <- self$searchWSItemID(itemPath = folderPath)
      if(is.null(pathID)){
        errMsg <- sprintf("No folder for path '%s'", folderPath)
        self$ERROR(errMsg)
        stop(errMsg)
      }
      
      absolutefile <- tools:::file_path_as_absolute(file)
      wdfile <- file.path(getwd(), basename(file))
      localfile <- absolutefile
      
      type <- ifelse(archive, "ARCHIVE", "FILE")
      
      upload_url <- sprintf("%s/items/%s/create/%s", private$url_storagehub, pathID, type)
      
      upload_req <- switch(private$token_type,
        "gcube" = {
          upload_url <- paste0(upload_url, "?gcube-token=", self$getToken())
          if(!self$verbose.debug){
            httr::POST(
              url = upload_url,
              body = list(
                name = name,
                description = description,
                file = httr::upload_file(file)
              )
            )
          }else{
            httr::with_verbose(
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
        },
        "jwt" = {
          if(!self$verbose.debug){
            httr::POST(
              url = upload_url,
              body = list(
                name = name,
                description = description,
                file = httr::upload_file(file)
              ),
              httr::add_headers("Authorization" = paste("Bearer", self$getToken()))
            )
          }else{
            httr::with_verbose(
              httr::POST(
                url = upload_url,
                body = list(
                  name = name,
                  description = description,
                  file = httr::upload_file(file)
                ),
                httr::add_headers("Authorization" = paste("Bearer", self$getToken()))
              )
            )
          }
        }
      )
      
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
    },
    
    #'@description Deletes an item given its path on the workspace
    #'@param itemPath item path
    #'@param force whether to force deletion, default is \code{FALSE}
    #'@return \code{TRUE} if deleted, \code{FALSE} otherwise
    deleteItem = function(itemPath, force = FALSE){
      deleted <- FALSE
      pathID <- self$searchWSItemID(itemPath = itemPath)
      if(!is.null(pathID)){
        delete_url <- sprintf("%s/items/%s", private$url_storagehub, pathID)
        if(force){
          self$INFO(sprintf("Deleting item '%s' (ID = %s) - 'force' is true, deleting permanently!", itemPath, pathID))
          delete_url <- sprintf("%s/items/%s?force=true", private$url_storagehub, pathID)
        }else{
          self$INFO(sprintf("Deleting item '%s' (ID = %s) - moving to trash!", itemPath, pathID))
        }
        delete_req <- switch(private$token_type,
          "gcube" = {
            delete_url <- paste0(delete_url, ifelse(force, "&", "?"), "gcube-token=", self$getToken())
            if(!self$verbose.debug){
              httr::DELETE(url = delete_url)
            }else{
              httr::with_verbose(httr::DELETE(url = delete_url))
            }
          },
          "jwt" = {
            if(!self$verbose.debug){
              httr::DELETE(url = delete_url, httr::add_headers("Authorization" = paste("Bearer", self$getToken())))
            }else{
              httr::with_verbose(httr::DELETE(url = delete_url, httr::add_headers("Authorization" = paste("Bearer", self$getToken()))))
            }
          }
        )
        if(httr::status_code(delete_req)==200){
          self$INFO("Successfully deleted item!")
          deleted <- TRUE
        }else{
          errMsg <- sprintf("Error while trying to delete file '%s' (ID = )", itemPath, pathID)
          self$ERROR(errMsg)
          stop(errMsg)
        }
      }else{
        self$WARN(sprintf("No item for path '%s'. Nothing deleted!", itemPath))
        deleted <- FALSE
      }
      return(deleted)
    },
    
    #'@description Shares an item with users
    #'@param itemPath item path
    #'@param defaultAccessType access type to use for sharing, among 'WRITE_ALL', 'WRITE_OWNER', 'READ_ONLY', 'ADMINISTRATOR'
    #'@param users one or more user names with whom the item has to be shared
    #'@return \code{TRUE} if shared, \code{FALSE} otherwise
    shareItem = function(itemPath, defaultAccessType, users){
      
      supportedDefaultAccessTypes <- c("WRITE_ALL", "WRITE_OWNER", "READ_ONLY", "ADMINISTATOR")
      if(!defaultAccessType %in% supportedDefaultAccessTypes){
        errMsg <- sprintf("Unsupported default access type '%s'. Supported values are [%s]", 
                          defaultAccessType, paste0(supportedDefaultAccessTypes, collapse=","))
        self$ERROR(errMsg)
        stop(errMsg)
      }
      
      body <- list(defaultAccessType = defaultAccessType)
      the_users <- sapply(users, function(x){list(users = x)})
      names(the_users) <- rep("users", length(the_users))
      body <- c(body, the_users)
      
      shared <- FALSE
      pathID <- self$searchWSItemID(itemPath = itemPath)
      if(!is.null(pathID)){
        share_url <- sprintf("%s/items/%s/share", private$url_storagehub, pathID)
        shared_req <- switch(private$token_type,
         "gcube" = {
           share_url <-paste0(share_url, "?gcube-token=", self$getToken())
           httr::PUT(
             share_url, 
             encode = "multipart", 
             body = body
           )
         },
         "jwt" = {
           httr::PUT(
             share_url, 
             httr::add_headers("Authorization" = paste("Bearer", self$getToken())),
             encode = "multipart",
             body = body
           )
         }
        )
        if(!is.null(shared_req)) if(httr::status_code(shared_req)==200){
          shared <- TRUE
        }
      }else{
        self$WARN(sprintf("No item for path '%s'. Nothing to share!", itemPath))
        shared <- FALSE
      }
      return(shared)
    },
    
    #'@description unshare an item
    #'@param itemPath item path
    #'@param users users
    #'@return \code{TRUE} if unshared, \code{FALSE} otherwise
    unshareItem = function(itemPath, users){
      
      the_users <- sapply(users, function(x){list(users = x)})
      names(the_users) <- rep("users", length(the_users))
      body <- c(the_users)
      
      pathID <- self$searchWSItemID(itemPath = itemPath)
      if(!is.null(pathID)){
        share_url <- sprintf("%s/items/%s/unshare", private$url_storagehub, pathID)
        shared_req <- switch(private$token_type,
         "gcube" = {
           share_url <-paste0(share_url, "?gcube-token=", self$getToken())
           httr::PUT(
             share_url, 
             encode = "multipart", 
             body = body
           )
         },
         "jwt" = {
           httr::POST(
             share_url, 
             httr::add_headers("Authorization" = paste("Bearer", self$getToken())),
             encode = "multipart",
             body = body
           )
         }
        )
        if(!is.null(shared_req)) if(httr::status_code(shared_req)==200){
          shared <- TRUE
        }
      }else{
        self$WARN(sprintf("No item for path '%s'. Nothing to share!", itemPath))
        shared <- FALSE
      }
      return(shared)
    },
    
    #'@description Get public file link
    #'@param path file path
    #'@return the public file URL
    getPublicFileLink = function(path){
      link <- NULL
      pathID <- self$searchWSItemID(itemPath = path)
      if(is.null(pathID)){
        errMsg <- sprintf("No file for path '%s'", path)
        self$ERROR(errMsg)
        stop(errMsg)
      }
      link_url <- sprintf("%s/items/%s/publiclink?exclude=hl:accounting", private$url_storagehub, pathID)
      pl_req <- switch(private$token_type,
        "gcube" = {
          link_url <-paste0(link_url, "&gcube-token=", self$getToken())
          httr::GET(link_url)
        },
        "jwt" = {
          httr::GET(link_url, httr::add_headers("Authorization" = paste("Bearer", self$getToken())))
        }
      )
      if(!is.null(pl_req)) link <- httr::content(pl_req)
      return(link)
    }
    
  )
)
