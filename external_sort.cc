#include "external_sort.hh"

#include <seastar/core/sharded.hh>

#include "app_config.hh"
#include "first_pass_service.hh"
#include "second_pass_service.hh"

seastar::future<> external_sort(const app_config &config) {
    logger.info("Starting external sort on file : {}", config.input_filename);

    seastar::sharded<first_pass_service> fps;
    seastar::sharded<second_pass_service> sps;
    seastar::sharded<second_pass_service> final_ps;

    try {
        // initialize the first pass service across shards
        co_await fps.start(config.input_filename, config.temp_working_dir);

        logger.info("Running first pass");

        // run the first pass
        co_await fps.invoke_on_all([](first_pass_service &local_service) {
            return local_service.run();
        });

        logger.info("Completed first pass");
        logger.info("Running second pass");

        // initialize the second pass service across shards
        auto get_num_of_files = [](const first_pass_service &fps) {
            return fps.get_total_files();
        };
        co_await sps.start(
            config.temp_working_dir,
            seastar::sharded_parameter(get_num_of_files, std::ref(fps)));

        // run the second pass
        co_await sps.invoke_on_all([](second_pass_service &local_service) {
            return local_service.run();
        });

        logger.info("Completed second pass");
        logger.info("Running a final pass merging all intermediate files into "
                    "a single sorted file");

        // initialize the final pass
        co_await final_ps.start(config.temp_working_dir, seastar::smp::count,
                                config.output_filename);
        // run it either locally or on another shard if available
        co_await final_ps.invoke_on((seastar::smp::count > 1 ? 1 : 0),
                                    [](second_pass_service &local_service) {
                                        return local_service.run();
                                    });

        logger.info("Completed sorting the given file");
        logger.info("Sorted file is stored at : {}", config.output_filename);

    } catch (...) {
        logger.error("external sort failed with following error : {}",
                     std::current_exception());
    }

    co_await fps.stop();
    co_await sps.stop();
    co_await final_ps.stop();
}
