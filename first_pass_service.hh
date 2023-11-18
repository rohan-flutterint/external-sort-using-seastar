#pragma once

#include <seastar/core/file.hh>
#include <seastar/core/sharded.hh>

#include "common.hh"

// Service to read a subset of the file, split them into batches and sort them
// in-memory
class first_pass_service : public seastar::sharded<first_pass_service> {
    seastar::sstring _filename, _tempdir;
    seastar::file _f;
    unsigned _temp_file_id{0};

    // this batch has to sort strings from _start_offset to _end_offset
    uint64_t _start_offset;
    uint64_t _end_offset;

    // opens the file and initialises the offsets for this shard
    seastar::future<> init();
    // write the given prority queue into a temp file in disk
    seastar::future<> write_pq_to_temp_file(record_priority_queue &pq);

  public:
    first_pass_service(const seastar::sstring &filename,
                       const seastar::sstring &tempdir)
        : _filename(filename), _tempdir(tempdir) {}

    unsigned int get_total_files() const { return _temp_file_id; }

    // first pass splits the given part of the file into batches and then
    // individually sorts them and writes them into disk.
    seastar::future<> run();

    seastar::future<> stop();
};
