add_library(common_jsonparsers
    ColumnObjectParser.cpp
    # DummyJSONParser.cpp
    # SimdJSONParser.cpp
)
target_include_directories(common_jsonparsers PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
