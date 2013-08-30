message( STATUS "osdc is here" )

set(Journaler Journaler.cc)

add_library(Journaler STATIC ${Journaler})

set(objectcacher ObjectCacher.cc)

add_library(objectcacher STATIC ${objectcacher})

set(objecter Objecter.cc)

add_library(objecter STATIC ${objecter})

set(striper Striper.cc)

add_library(striper STATIC ${striper})
