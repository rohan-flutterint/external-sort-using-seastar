#pragma once

#include <queue>

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/smp.hh>
#include <seastar/coroutine/generator.hh>
#include <seastar/util/log.hh>

// forward declarations
namespace seastar {
class file;
} // namespace seastar

constexpr size_t record_size = 4 * 1024; // 4K bytes

// TODO : check if this affects the performance somehow
constexpr size_t max_buffer_size_for_read = 100;

extern seastar::logger logger;

using record = seastar::temporary_buffer<char>;

// clang has some issues with template with default args.
// create an alias and trick it
template <typename T> using circular_buffer = seastar::circular_buffer<T>;
using buffered_record_generator =
    seastar::coroutine::experimental::generator<record, circular_buffer>;

using unbuffered_record_generator =
    seastar::coroutine::experimental::generator<record, std::optional>;

// TODO: using a round buffer fails with an assert
// seastar::internal::future_base::do_wait(): Assertion `thread' failed -
// fix that later - use unbuffered generator for now
using record_generator = unbuffered_record_generator;

// creates a generator that read the records one by one from the file
record_generator get_record_iterator(
    seastar::coroutine::experimental::buffer_size_t max_buffer_size,
    seastar::file &f, uint64_t start_offset = 0, uint64_t end_offset = 0);

// comparator for records
class record_greater {
  public:
    bool operator()(record &a, record &b) {
        return strncmp(a.get(), b.get(), record_size) > 0;
    }
};

// returns the name of the intermediate files produced by first pass
seastar::sstring inline generate_first_pass_output_file_name(
    const seastar::sstring &tempdir, const unsigned int file_id) {
    return tempdir + "/sorted_batch_" +
           std::to_string(seastar::this_shard_id()) + "_" +
           std::to_string(file_id);
}

using record_priority_queue =
    std::priority_queue<record, std::vector<record>, record_greater>;
