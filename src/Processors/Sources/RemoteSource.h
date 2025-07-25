#pragma once

#include <Processors/ISimpleTransform.h>
#include <Processors/ISource.h>
#include <Processors/RowsBeforeStepCounter.h>
#include <QueryPipeline/Pipe.h>

#include <Core/UUID.h>

#include <Common/EventFD.h>

namespace DB
{

class RemoteQueryExecutor;
using RemoteQueryExecutorPtr = std::shared_ptr<RemoteQueryExecutor>;

/// Source from RemoteQueryExecutor. Executes remote query and returns query result chunks.
class RemoteSource final : public ISource
{
public:
    /// Flag add_aggregation_info tells if AggregatedChunkInfo should be added to result chunk.
    /// AggregatedChunkInfo stores the bucket number used for two-level aggregation.
    /// This flag should be typically enabled for queries with GROUP BY which are executed till WithMergeableState.
    RemoteSource(RemoteQueryExecutorPtr executor, bool add_aggregation_info_, bool async_read_, bool async_query_sending_);
    ~RemoteSource() override;

    Status prepare() override;
    void work() override;
    String getName() const override { return "Remote"; }

    void setRowsBeforeLimitCounter(RowsBeforeStepCounterPtr counter) override { rows_before_limit.swap(counter); }
    void setRowsBeforeAggregationCounter(RowsBeforeStepCounterPtr counter) override { rows_before_aggregation.swap(counter); }

    /// Stop reading from stream if output port is finished.
    void onUpdatePorts() override;

    int schedule() override;

    void onAsyncJobReady() override;

    void setStorageLimits(const std::shared_ptr<const StorageLimitsList> & storage_limits_) override;

protected:
    std::optional<Chunk> tryGenerate() override;
    void onCancel() noexcept override;

private:
    std::atomic_bool was_query_sent = false;
    bool need_drain = false;
    bool executor_finished = false;
    bool add_aggregation_info = false;
    RemoteQueryExecutorPtr query_executor;
    RowsBeforeStepCounterPtr rows_before_limit;
    RowsBeforeStepCounterPtr rows_before_aggregation;

    const bool async_read;
    const bool async_query_sending;
    bool is_async_state = false;
    int fd = -1;
    size_t rows = 0;
    bool manually_add_rows_before_limit_counter = false;
    std::atomic_bool preprocessed_packet = false;
#if defined(OS_LINUX)
    EventFD startup_event_fd;
#endif
};

/// Totals source from RemoteQueryExecutor.
class RemoteTotalsSource : public ISource
{
public:
    explicit RemoteTotalsSource(RemoteQueryExecutorPtr executor);
    ~RemoteTotalsSource() override;

    String getName() const override { return "RemoteTotals"; }

protected:
    Chunk generate() override;

private:
    RemoteQueryExecutorPtr query_executor;
};

/// Extremes source from RemoteQueryExecutor.
class RemoteExtremesSource : public ISource
{
public:
    explicit RemoteExtremesSource(RemoteQueryExecutorPtr executor);
    ~RemoteExtremesSource() override;

    String getName() const override { return "RemoteExtremes"; }

protected:
    Chunk generate() override;

private:
    RemoteQueryExecutorPtr query_executor;
};

struct UnmarshallBlocksTransform : ISimpleTransform
{
public:
    explicit UnmarshallBlocksTransform(SharedHeader header_)
        : ISimpleTransform(header_, header_, false)
    {
    }

    String getName() const override { return "UnmarshallBlocksTransform"; }

    void transform(Chunk & chunk) override;
};

/// Create pipe with remote sources.
Pipe createRemoteSourcePipe(
    RemoteQueryExecutorPtr query_executor,
    bool add_aggregation_info,
    bool add_totals,
    bool add_extremes,
    bool async_read,
    bool async_query_sending,
    size_t parallel_marshalling_threads);
}
