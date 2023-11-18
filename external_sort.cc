#include "external_sort.hh"

#include <seastar/core/sharded.hh>

#include "first_pass_service.hh"
#include "second_pass_service.hh"

seastar::future<> external_sort(const seastar::sstring filename,
                                const seastar::sstring tempdir) {
    logger.info("Starting external sort on file : {}", filename);

    seastar::sharded<first_pass_service> fps;
    seastar::sharded<second_pass_service> sps;
    seastar::sharded<second_pass_service> final_ps;

    try {
        // initialize the first pass service across shards
        co_await fps.start(filename, tempdir);

        // run the first pass
        co_await fps.invoke_on_all([](first_pass_service &local_service) {
            return local_service.run();
        });

        // initialize the second pass service across shards
        auto get_num_of_files = [](const first_pass_service &fps) {
            return fps.get_total_files();
        };
        co_await sps.start(tempdir, seastar::sharded_parameter(get_num_of_files,
                                                               std::ref(fps)));

        // run the second pass
        co_await sps.invoke_on_all([](second_pass_service &local_service) {
            return local_service.run();
        });

        // initialize the final pass
        co_await final_ps.start(tempdir, seastar::smp::count, filename);
        // run it either locally or on another shard if available
        co_await final_ps.invoke_on((seastar::smp::count > 1 ? 1 : 0),
                                    [](second_pass_service &local_service) {
                                        return local_service.run();
                                    });

    } catch (...) {
        logger.error("external sort failed with following error : {}",
                     std::current_exception());
    }

    co_await fps.stop();
    co_await sps.stop();
    co_await final_ps.stop();
}
