set(osdc_files
	../osdc/Objecter.cc 
	../osdc/ObjectCacher.cc 
	../osdc/Filer.cc 
	../osdc/Striper.cc
)

include_directories("..")

add_library(osdc ${osdc_files})

message( STATUS "osdc is here" )
