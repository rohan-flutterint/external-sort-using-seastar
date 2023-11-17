#include "external_sort.hh"

#include <seastar/core/sharded.hh>

#include "first_pass_service.hh"

seastar::future<> external_sort(const seastar::sstring filename,
                                const seastar::sstring tempdir) {
    logger.info("Starting external sort on file : {}", filename);

    seastar::sharded<first_pass_service> fps;

    try {
        // initialize the first pass service across shards
        co_await fps.start(filename, tempdir);

        // run the first pass
        co_await fps.invoke_on_all([](first_pass_service &local_service) {
            return local_service.run();
        });

    } catch (...) {
        logger.error("external sort failed with following error : {}",
                     std::current_exception());
    }

    co_await fps.stop();
}
