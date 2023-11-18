
#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>

#include "common.hh"

// Service that runs the second pass of the external sort - this run merges the
// records from given files into a single file
const seastar::sstring default_sstring;
class second_pass_service {
    // this service will be run twice - first to merge the files per shard, when
    // final_run is false and then to merge files across shard when final_run is
    // true
    bool _final_run{false};
    unsigned int _number_of_files;

    seastar::sstring _tempdir, _output_filename;

    record_queue_vector _record_queues;
    seastar::semaphore _record_queues_consumed{0};

    // function wrapper for the input name generator
    std::function<seastar::sstring(const seastar::sstring &,
                                   const unsigned int)>
        _generate_input_filename;

    // open the batch files and setup read via queues.
    // note - unable to write this as a lambda due to the
    // 'lambda-coroutine-fiasco'.
    seastar::future<> setup_read_from_files(unsigned int file_id);

  public:
    second_pass_service(const seastar::sstring &tempdir,
                        unsigned int number_of_files,
                        const seastar::sstring &filename = default_sstring)
        : _tempdir(tempdir), _number_of_files(number_of_files) {
        if (filename.empty()) {
            // second pass
            _output_filename = generate_second_pass_output_file_name(_tempdir);
            _generate_input_filename = generate_first_pass_output_file_name;
        } else {
            // final pass
            _final_run = true;
            _output_filename = filename + ".sorted";
            _generate_input_filename = generate_second_pass_output_file_name;
        }
    }

    seastar::future<> run();
    seastar::future<> stop();
};
