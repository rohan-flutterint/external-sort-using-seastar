#include "verify_service.hh"
#include "common.hh"

seastar::future<> verify_service::run() {
    // open the file and init offsets for the shard
    co_await init();

    // To ensure that the records handled by this shard are also sorted
    // lexicographically w.r.to the records handled by other shards, include two
    // additional records - one before the start_offset and one after the
    // end_offset when verification is done. When each shard is in order within
    // itself and w.r.to these two additional records, the whole file will be in
    // order.
    if (seastar::this_shard_id() != 0) {
        // skip decreasing offset in the first shard
        _start_offset -= record_size;
    }

    if (seastar::this_shard_id() != seastar::smp::count - 1) {
        // skip increasing offset in the last shard
        _end_offset += record_size;
    }

    logger.debug("started verifying the result {} {}", _start_offset,
                 _end_offset);

    record prev_record;
    record_greater record_greater_;
    while (_start_offset < _end_offset) {
        // use generator to read the records one by one
        // and compare
        auto records = get_record_iterator(
            seastar::coroutine::experimental::buffer_size_t{
                max_buffer_size_for_read},
            _f, _start_offset, _end_offset);

        int records_read = 0;

        if (prev_record.size() == 0) {
            prev_record = std::move(*(co_await records()));
            records_read++;
        }

        while (auto record = co_await records()) {
            if (record_greater_(prev_record, *record)) {
                // wrong sort order
                throw verification_exception("file is incorrectly sorted");
            }
            prev_record = std::move(*record);
            records_read++;
        }

        // generator stopped either due to reaching offset
        // or due to running out of memory in this shard.
        // update the _start_offset to mark the number of records read.
        _start_offset += records_read * record_size;
    }

    // at this point it is verified that the data alloted to this shard is
    // completely sorted within itself and compared to the previous and the next
    // shards. i.e. SUCCESS!
}
