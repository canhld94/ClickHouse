#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


class StorageSystemModels final : public ext::shared_ptr_helper<StorageSystemModels>, public IStorageSystemOneBlock<StorageSystemModels>
{
    friend struct ext::shared_ptr_helper<StorageSystemModels>;
public:
    std::string getName() const override { return "SystemModels"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
