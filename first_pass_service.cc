#include "first_pass_service.hh"

#include <seastar/core/seastar.hh>

#include "common.hh"

seastar::future<> first_pass_service::init() {
    _f = co_await seastar::open_file_dma(_filename, seastar::open_flags::ro);
    auto file_size = co_await _f.size();
    assert(file_size % record_size == 0);
    const auto total_records = file_size / record_size;

    // TODO : handle case where total_records < seastar::smp::count
    // and also the case where the entire file can fit in memory
    assert(total_records > seastar::smp::count);

    // Deduce number of records that need to be handled by this shard.
    // We equally divide the number of records across all the shards
    // and then redistribute any remainder.
    const auto shard_id = seastar::this_shard_id();
    auto records_for_this_shard = total_records / seastar::smp::count;
    auto start_offset_record = records_for_this_shard * shard_id;

    // handle the remainder
    const auto remainder = total_records % seastar::smp::count;
    if (shard_id < remainder) {
        // pick one more record for this shard
        records_for_this_shard++;
    }
    start_offset_record += std::min(shard_id, (unsigned)remainder);

    // calculate the actual offsets in the file
    _start_offset = start_offset_record * record_size;
    _end_offset = _start_offset + (records_for_this_shard * record_size) - 1;
    logger.debug("start offset {} and end offset {}", _start_offset,
                 _end_offset);
}

seastar::future<>
first_pass_service::write_pq_to_temp_file(record_priority_queue &pq) {
    // create the temp file and allocate space
    auto temp_file_name =
        generate_first_pass_output_file_name(_tempdir, _temp_file_id++);
    auto f = co_await seastar::open_file_dma(
        temp_file_name, seastar::open_flags::wo | seastar::open_flags::create);
    co_await f.allocate(0, pq.size() * record_size);

    // pop the elements and write them into a temp file
    uint64_t write_offset = 0;
    while (!pq.empty()) {
        co_await f.dma_write<char>(write_offset, pq.top().get(), record_size);
        write_offset += record_size;
        pq.pop();
    }

    co_await f.close();
}

seastar::future<> first_pass_service::run() {
    // open the file and init offsets for the shard
    co_await init();

    logger.debug("starting first pass");

    record_priority_queue pq;
    unsigned num_of_records = 0;
    while (_start_offset < _end_offset) {
        // use generator to read the records one by one
        auto records = get_record_iterator(
            seastar::coroutine::experimental::buffer_size_t{
                max_buffer_size_for_read},
            _f, _start_offset, _end_offset);

        // in parallel, push it into a priority_queue
        // and sort it within this batch
        while (auto record = co_await records()) {
            pq.push(std::move(*record));
        }

        // generator stopped either due to reaching offset
        // or due to running out of memory in this shard.
        // update the _start_offset to mark the number of records read.
        _start_offset += (pq.size()) * record_size;
        num_of_records += pq.size();

        // save the sorted records in the pq in a temp file
        // TODO : check if the reading can be done in parallel once the write
        // starts
        co_await write_pq_to_temp_file(pq);
    }

    logger.debug("first pass completed : sorted {} entries into {} batches",
                 num_of_records, _temp_file_id);

    // close the input file
    co_await _f.close();
}

seastar::future<> first_pass_service::stop() {
    logger.debug("stopping service");
    // close the file, if it is still open - might happen on exceptions
    if (_f) {
        co_await _f.close();
    }
}
