message( STATUS "librados is here" )

set(librados librados.cc)

add_library(librados STATIC ${librados})

set(radosclient RadosClient.cc)

add_library(radosclient STATIC ${radosclient})

set(ioxctximpl IoCtxImpl.cc)

add_library(ioxctximpl STATIC ${ioxctximpl})

