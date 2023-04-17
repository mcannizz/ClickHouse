#pragma once

#include <Processors/IProcessor.h>


namespace DB
{

/// SourceFromSingleChunk' big brother wich supports multiple chunks and totals/extremes.
class SourceFromChunks : public IProcessor
{
public:
    /// Disambiguates the meaning of the "other" parameter in the ctor
    enum class OtherTag
    {
        Totals,
        Extremes
    };

    SourceFromChunks(Block header, Chunks chunks_);
    SourceFromChunks(Block header, Chunks chunks_, Chunks other, OtherTag tag);
    SourceFromChunks(Block header, Chunks chunks_, Chunks totals_, Chunks extremes_);

    SourceFromChunks(Block header, std::shared_ptr<Chunks> chunks_);
    SourceFromChunks(Block header, std::shared_ptr<Chunks> chunks_, std::shared_ptr<Chunks> other_, OtherTag tag);
    SourceFromChunks(Block header, std::shared_ptr<Chunks> chunks_, std::shared_ptr<Chunks> totals_, std::shared_ptr<Chunks> extremes_);

    ~SourceFromChunks() override = default;

    String getName() const override;

    Status prepare() override;
    void work() override;

    void setStorageLimits(const std::shared_ptr<const StorageLimitsList> & storage_limits_) override;
    std::optional<ReadProgress> getReadProgress() final;
    void progress(size_t read_rows, size_t read_bytes);

private:
    Chunk generate();

    ReadProgressCounters read_progress;
    bool read_progress_was_set = false;
    OutputPort & output;
    OutputPort * output_totals = nullptr;
    OutputPort * output_extremes = nullptr;
    bool has_input = false;
    bool finished = false;
    bool got_exception = false;
    Port::Data current_chunk;
    const std::shared_ptr<Chunks> chunks;
    const std::shared_ptr<Chunks> chunks_totals;
    const std::shared_ptr<Chunks> chunks_extremes;
    Chunks::iterator it; // TODO multiple iterators
    const bool move_from_chunks; /// Optimization: if the chunks are exclusively owned by SourceFromChunks, then generate() can move from them
};

}
