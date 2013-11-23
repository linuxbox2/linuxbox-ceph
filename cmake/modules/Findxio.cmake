# - Find libxio
# Find libxio transport library
#
# Xio_INCLUDE_DIR -  libxio include dir
# Xio_LIBRARIES - List of libraries
# Xio_FOUND - True if libxio found.

find_path(Xio_INCLUDE_DIR libxio.h NO_DEFAULT_PATH PATHS
  ${HT_DEPENDENCY_INCLUDE_DIR}
  /usr/include
  /usr/local/include
  /opt/accelio/include
)

find_library(Xio_LIBRARY NO_DEFAULT_PATH
  NAMES xio
  PATHS ${HT_DEPENDENCY_LIB_DIR} /lib /usr/lib /usr/local/lib /opt/accelio/lib
)

if (Xio_INCLUDE_DIR AND Xio_LIBRARY)
  set(Xio_FOUND TRUE)
  set(Xio_LIBRARIES ${Xio_LIBRARY} )
else ()
  set(Xio_FOUND FALSE)
  set(Xio_LIBRARIES )
endif ()

if (Xio_FOUND)
  message(STATUS "Found Xio: ${Xio_LIBRARY}")
else ()
  message(STATUS "Not Found Xio: ${Xio_LIBRARY}")
  if (Xio_FIND_REQUIRED)
    message(STATUS "Looked for Xio libraries named ${Xio_NAMES}.")
    message(FATAL_ERROR "Could NOT find Xio library")
  endif ()
endif ()

mark_as_advanced(
  Xio_LIBRARY
  Xio_INCLUDE_DIR
  )
