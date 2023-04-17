#include <Processors/Sources/SourceFromChunks.h>

#include <QueryPipeline/StreamLocalLimits.h>


namespace DB
{

SourceFromChunks::SourceFromChunks(Block header, Chunks chunks_)
    : IProcessor({}, {header})
    , output(*outputs.begin())
    , chunks(std::make_shared<Chunks>(std::move(chunks_)))
    , it(chunks->begin())
    , move_from_chunks(true)
{
}

SourceFromChunks::SourceFromChunks(Block header, Chunks chunks_, Chunks other, OtherTag tag)
    : IProcessor({}, {header, header})
    , output(*outputs.begin())
    , output_totals(tag == OtherTag::Totals ? &*++outputs.begin() : nullptr)
    , output_extremes(tag == OtherTag::Totals ? nullptr : &*++outputs.begin())
    , chunks(std::make_shared<Chunks>(std::move(chunks_)))
    , chunks_totals(tag == OtherTag::Totals ? std::make_shared<Chunks>(std::move(other)) : nullptr)
    , chunks_extremes(tag == OtherTag::Totals ? nullptr : std::make_shared<Chunks>(std::move(other)))
    , it(chunks->begin())
    , move_from_chunks(true)
{
}

SourceFromChunks::SourceFromChunks(Block header, Chunks chunks_, Chunks totals_, Chunks extremes_)
    : IProcessor({}, {header, header, header})
    , output(*outputs.begin())
    , output_totals(&*++outputs.begin())
    , output_extremes(&*++++outputs.begin())
    , chunks(std::make_shared<Chunks>(std::move(chunks_)))
    , chunks_totals(std::make_shared<Chunks>(std::move(totals_)))
    , chunks_extremes(std::make_shared<Chunks>(std::move(extremes_)))
    , it(chunks->begin())
    , move_from_chunks(true)
{
}

SourceFromChunks::SourceFromChunks(Block header, std::shared_ptr<Chunks> chunks_)
    : IProcessor({}, {header})
    , output(*outputs.begin())
    , chunks(chunks_)
    , it(chunks->begin())
    , move_from_chunks(false)
{
    chassert(chunks);
}

SourceFromChunks::SourceFromChunks(Block header, std::shared_ptr<Chunks> chunks_, std::shared_ptr<Chunks> other_, OtherTag tag)
    : IProcessor({}, {header, header})
    , output(*outputs.begin())
    , output_totals(tag == OtherTag::Totals ? &*++outputs.begin() : nullptr)
    , output_extremes(tag == OtherTag::Totals ? nullptr : &*++outputs.begin())
    , chunks(chunks_)
    , chunks_totals(tag == OtherTag::Totals ? other_ : nullptr)
    , chunks_extremes(tag == OtherTag::Totals ? nullptr : other_)
    , it(chunks->begin())
    , move_from_chunks(false)
{
    chassert(chunks);
    if (tag == OtherTag::Totals)
        chassert(chunks_totals);
    else
        chassert(chunks_extremes);
}

SourceFromChunks::SourceFromChunks(Block header, std::shared_ptr<Chunks> chunks_, std::shared_ptr<Chunks> totals_, std::shared_ptr<Chunks> extremes_)
    : IProcessor({}, {header, header, header})
    , output(*outputs.begin())
    , output_totals(&*++outputs.begin())
    , output_extremes(&*++++outputs.begin())
    , chunks(chunks_)
    , chunks_totals(totals_)
    , chunks_extremes(extremes_)
    , it(chunks->begin())
    , move_from_chunks(false)
{
    chassert(chunks);
    chassert(chunks_totals);
    chassert(chunks_extremes);
}

String SourceFromChunks::getName() const
{
    return "SourceFromChunks";
}

SourceFromChunks::Status SourceFromChunks::prepare()
{
    // TODO
    if (output_totals)
        output_totals->finish();
    if (output_extremes)
        output_extremes->finish();

    if (finished)
    {
        output.finish();
        return Status::Finished;
    }

    /// Check can output.
    if (output.isFinished())
        return Status::Finished;

    if (!output.canPush())
        return Status::PortFull;

    if (!has_input)
        return Status::Ready;

    output.pushData(std::move(current_chunk));
    has_input = false;

    if (isCancelled())
    {
        output.finish();
        return Status::Finished;
    }

    if (got_exception)
    {
        finished = true;
        output.finish();
        return Status::Finished;
    }

    /// Now, we pushed to output, and it must be full.
    return Status::PortFull;
}

void SourceFromChunks::work()
{
    try
    {
        read_progress_was_set = false;

        if (auto chunk = generate())
        {
            current_chunk.chunk = std::move(chunk);
            if (current_chunk.chunk)
            {
                has_input = true;
                if (!read_progress_was_set)
                    progress(current_chunk.chunk.getNumRows(), current_chunk.chunk.bytes());
            }
        }
        else
            finished = true;

        if (isCancelled())
            finished = true;
    }
    catch (...)
    {
        finished = true;
        got_exception = true;
        throw;
    }
}

void SourceFromChunks::setStorageLimits(const std::shared_ptr<const StorageLimitsList> & /*storage_limits_*/)
{
    /// Should we bother?
}

void SourceFromChunks::progress(size_t read_rows, size_t read_bytes)
{
    read_progress_was_set = true;
    read_progress.read_rows += read_rows;
    read_progress.read_bytes += read_bytes;
}

std::optional<SourceFromChunks::ReadProgress> SourceFromChunks::getReadProgress()
{
    if (finished && read_progress.read_bytes == 0 && read_progress.total_rows_approx == 0)
        return {};

    ReadProgressCounters res_progress;
    std::swap(read_progress, res_progress);

    static StorageLimitsList empty_limits;
    return ReadProgress{res_progress, empty_limits};
}

Chunk SourceFromChunks::generate()
{
    if (it != chunks->end())
        if (move_from_chunks)
        {
            Chunk && chunk = std::move(*it);
            it++;
            return chunk;
        }
        else
        {
            Chunk chunk = it->clone();
            it++;
            return chunk;
        }
    else
        return {};
}

}

