include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(clickhouse_functions_embedding .)

# ONNX/llama backend sources are opt-in; exclude from the default glob so
# configurations without the optional contribs still build.
list(FILTER clickhouse_functions_embedding_sources EXCLUDE REGEX ".*/OnnxEmbeddingModel\\.cpp$")
list(FILTER clickhouse_functions_embedding_sources EXCLUDE REGEX ".*/LlamaCppEmbeddingModel\\.cpp$")

add_library(clickhouse_functions_embedding OBJECT ${clickhouse_functions_embedding_sources} ${clickhouse_functions_embedding_headers})
target_link_libraries(clickhouse_functions_embedding PRIVATE dbms ch_contrib::icu ch_contrib::abseil_swiss_tables ch_contrib::rapidjson)

if (TARGET ch_contrib::simsimd)
    target_link_libraries(clickhouse_functions_embedding PRIVATE ch_contrib::simsimd)
    target_compile_definitions(clickhouse_functions_embedding PRIVATE USE_SIMSIMD=1)
endif()

if (TARGET ch_contrib::onnxruntime)
    target_sources(clickhouse_functions_embedding PRIVATE OnnxEmbeddingModel.cpp)
    target_include_directories(clickhouse_functions_embedding SYSTEM PRIVATE "${ONNXRUNTIME_INCLUDE_PATH}")
    target_include_directories(clickhouse_functions_embedding SYSTEM PRIVATE
        "${ClickHouse_SOURCE_DIR}/contrib/onnx"
        "${CMAKE_BINARY_DIR}/contrib/onnxruntime-cmake/generated")
    target_link_libraries(clickhouse_functions_embedding PRIVATE ch_contrib::onnxruntime)
    target_compile_definitions(clickhouse_functions_embedding PRIVATE USE_ONNXRUNTIME=1)
else()
    message(STATUS "Embedding: Dense ONNX functions disabled (ONNX Runtime not found)")
endif()

if (TARGET ch_contrib::llama_cpp)
    target_sources(clickhouse_functions_embedding PRIVATE LlamaCppEmbeddingModel.cpp)
    target_link_libraries(clickhouse_functions_embedding PRIVATE ch_contrib::llama_cpp)
    target_compile_definitions(clickhouse_functions_embedding PRIVATE USE_LLAMACPP=1)
else()
    message(STATUS "Embedding: llama.cpp backend disabled (llama.cpp not found)")
endif()

if (OMIT_HEAVY_DEBUG_SYMBOLS)
    target_compile_options(clickhouse_functions_embedding PRIVATE "-g0")
endif()

