#pragma once

#include "config.h"

#if USE_JEMALLOC

#    include <string>
#    include <unordered_map>
#    include <vector>
#    include <Core/SettingsEnums.h>
#    include <IO/ReadBufferFromFile.h>
#    include <Processors/ISource.h>

namespace DB
{

/// Source that reads a jemalloc heap profile file and outputs lines according to the requested format.
///
/// - Raw: streams lines directly from the file
/// - Symbolized: delegates to Jemalloc::symbolizeHeapProfileToBuffer, then streams the result
/// - Collapsed: parses the profile and emits FlameGraph-compatible collapsed stacks
class JemallocProfileSource : public ISource
{
public:
    JemallocProfileSource(
        const std::string & filename_,
        const SharedHeader & header_,
        size_t max_block_size_,
        JemallocProfileFormat mode_,
        bool symbolize_with_inline_);

    String getName() const override { return "JemallocProfile"; }

protected:
    Chunk generate() override;

private:
    Chunk generateRaw();
    Chunk generateSymbolized();
    Chunk generateCollapsed();

    std::string filename;
    std::unique_ptr<ReadBufferFromFile> file_input;
    size_t max_block_size;
    bool is_finished = false;
    JemallocProfileFormat mode;
    bool symbolize_with_inline;

    /// For Symbolized mode: all output lines buffered after one-shot symbolization
    std::vector<std::string> symbolized_lines;
    size_t current_symbolized_line = 0;

    /// For Collapsed mode: processed lines (need aggregation, can't stream)
    std::vector<std::string> collapsed_lines;
    size_t current_collapsed_line_index = 0;

    /// Cache for symbolized addresses to avoid redundant symbolization in Collapsed mode
    std::unordered_map<UInt64, std::vector<std::string>> symbolization_cache;
};

}

#endif
