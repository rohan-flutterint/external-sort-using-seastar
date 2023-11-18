#include "second_pass_service.hh"

#include <boost/iterator/counting_iterator.hpp>
#include <seastar/core/file.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/seastar.hh>

#include "common.hh"

seastar::future<> second_pass_service::stop() {
    logger.trace("stopping second_pass_service");
    co_return;
}

seastar::future<>
second_pass_service::setup_read_from_files(unsigned int file_id) {
    // open the temp batch file with the given id
    auto temp_file_name = _generate_input_filename(_tempdir, file_id);
    auto f = co_await seastar::open_file_dma(temp_file_name,
                                             seastar::open_flags::ro);
    auto end_offset = co_await f.size();

    // read the records one by one and push them into the queue
    record r;
    uint64_t start_offset = 0;
    seastar::queue<record> &record_queue = _record_queues[file_id];
    while (start_offset < end_offset) {
        auto r = co_await f.dma_read<char>(start_offset, record_size);
        co_await record_queue.push_eventually(std::move(r));
        start_offset += record_size;
    }

    // end of file - push an empty record to signal the consumer
    co_await record_queue.push_eventually(std::move(record()));

    // wait until all records are read and written
    co_await _record_queues_consumed.wait();

    co_await f.close();
    co_await seastar::remove_file(temp_file_name);
}

seastar::future<> second_pass_service::run() {
    logger.debug("starting second pass");

    if (_number_of_files <= 1) {
        assert(_number_of_files == 1 && !_final_run);
        // only one file exist - rename it to the form expected by the final run
        co_await seastar::rename_file(
            generate_first_pass_output_file_name(_tempdir, 0),
            generate_second_pass_output_file_name(_tempdir));
        co_await seastar::sync_directory(_tempdir);
        co_return;
    }

    // use a single queue per batch to read and write
    for (unsigned i = 0; i < _number_of_files; i++) {
        _record_queues.emplace_back(max_buffer_size_for_read);
    }

    // parallely populate the queues by reading from all the batch files
    auto setup_read_functor =
        std::bind(&second_pass_service::setup_read_from_files, this,
                  std::placeholders::_1);
    auto producers_future = seastar::parallel_for_each(
        boost::counting_iterator<unsigned>(0),
        boost::counting_iterator<unsigned>(_number_of_files),
        std::move(setup_read_functor));

    // populate the priority queue with the first entries from all the queues
    record_generator_priority_queue pq;
    co_await seastar::parallel_for_each(
        boost::counting_iterator<unsigned>(0),
        boost::counting_iterator<unsigned>(_number_of_files),
        [&pq, this](unsigned batch_file_id) -> seastar::future<> {
            seastar::queue<record> &record_queue =
                _record_queues[batch_file_id];
            auto r = co_await record_queue.pop_eventually();
            // note : all files atleast have one file - so no need to check for
            // entry validity here
            pq.emplace(std::move(r), batch_file_id);
        });

    // create the temp file
    auto f = co_await seastar::open_file_dma(_output_filename,
                                             seastar::open_flags::wo |
                                                 seastar::open_flags::create);

    // write from the pq into the single sorted file
    uint64_t write_offset = 0;
    while (!pq.empty()) {
        auto &t = pq.top();
        // write the top into file
        co_await f.dma_write<char>(write_offset, t.first.get(), record_size);
        write_offset += record_size;

        // pop the top and push the next record from the queue
        auto queue_id = t.second;
        pq.pop();

        // check if the queue has further entries
        seastar::queue<record> &record_queue = _record_queues[queue_id];
        auto r = co_await record_queue.pop_eventually();
        if (r.size() > 0) {
            // a record has been read from the queue
            pq.emplace(std::move(r), queue_id);
        }
    }

    // signal all the producers to complete
    _record_queues_consumed.signal(_number_of_files);
    co_await std::move(producers_future);

    // clenaup
    _record_queues.clear();
    co_await f.close();
    // sync tempdir to ensure that the pass1 temp file removals are flushed
    co_await seastar::sync_directory(_tempdir);

    logger.debug("completed second pass");
}
