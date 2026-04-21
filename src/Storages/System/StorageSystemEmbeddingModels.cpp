#include <Storages/System/StorageSystemEmbeddingModels.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/Embedding/EmbeddingModelRegistry.h>
#include <Common/logger_useful.h>

namespace DB
{

ColumnsDescription StorageSystemEmbeddingModels::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"id", std::make_shared<DataTypeString>(), "Model identifier from the server config."},
        {"type", std::make_shared<DataTypeString>(), "Model backend: onnx or llamacpp."},
        {"model_path", std::make_shared<DataTypeString>(), "Path to model file."},
        {"tokenizer_path", std::make_shared<DataTypeString>(), "Path to tokenizer file."},
        {"memory_bytes", std::make_shared<DataTypeUInt64>(), "Approximate memory usage in bytes."},
        {"metadata", std::make_shared<DataTypeString>(), "Model metadata as JSON."},
        {"architecture", std::make_shared<DataTypeString>(), "Architecture info as JSON."},
        {"quantization", std::make_shared<DataTypeString>(), "Quantization info as JSON."},
        {"tokenizer_info", std::make_shared<DataTypeString>(), "Tokenizer info as JSON."},
        {"runtime", std::make_shared<DataTypeString>(), "Runtime info as JSON."},
        {"stats", std::make_shared<DataTypeString>(), "Inference statistics as JSON."},
    };
}

void StorageSystemEmbeddingModels::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto & registry = EmbeddingModelRegistry::instance();

    auto add_model = [&](const String & id, const String & model_path, const String & tokenizer_path, IEmbeddingModel & model)
    {
        res_columns[0]->insert(id);
        res_columns[1]->insert(model.getTypeName());
        res_columns[2]->insert(model_path);
        res_columns[3]->insert(tokenizer_path);
        res_columns[4]->insert(static_cast<UInt64>(model.getMemoryBytes()));
        res_columns[5]->insert(model.getMetadataJSON());
        res_columns[6]->insert(model.getArchitectureJSON());
        res_columns[7]->insert(model.getQuantizationJSON());
        res_columns[8]->insert(model.getTokenizerJSON());
        res_columns[9]->insert(model.getRuntimeJSON());
        res_columns[10]->insert(model.getStatsJSON());
    };

    for (const auto & [id, entry] : registry.snapshotEntries())
    {
        if (entry.model)
            add_model(id, entry.model_path, entry.tokenizer_path, *entry.model);
    }
}

}
