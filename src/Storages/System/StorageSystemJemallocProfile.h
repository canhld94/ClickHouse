#pragma once

#include <Storages/IStorage.h>

namespace DB
{

/// System table that reads and displays the latest jemalloc heap profile
class StorageSystemJemallocProfile final : public IStorage
{
public:
    explicit StorageSystemJemallocProfile(const StorageID & table_id_);

    std::string getName() const override { return "SystemJemallocProfile"; }

    static ColumnsDescription getColumnsDescription();

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    bool isSystemStorage() const override { return true; }

    bool supportsTransactions() const override { return true; }
};

}
