#include "common.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>

seastar::logger logger("external-sort");

record_generator get_record_iterator(
    seastar::coroutine::experimental::buffer_size_t max_buffer_size,
    seastar::file &f, uint64_t start_offset, uint64_t end_offset) {

    if (end_offset <= 0) {
        // read the file until end
        end_offset = co_await f.size();
    }

    while (start_offset < end_offset) {
        try {
            co_yield co_await f.dma_read<char>(start_offset, record_size);
            start_offset += record_size;
        } catch (const std::bad_alloc &e) {
            // no more memory in this shard to load records
            break;
        } catch (...) {
            logger.error(
                "get_record_iterator generator failed with exception {}",
                std::current_exception());
            break;
        }
    }
}
