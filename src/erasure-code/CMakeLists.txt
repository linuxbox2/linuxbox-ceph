set(erasure_code_srcs
	ErasureCodePlugin.cc
)

add_library(erasure_code ${erasure_code_srcs})

target_link_libraries(erasure_code dl)
