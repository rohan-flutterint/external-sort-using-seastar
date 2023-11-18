#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/util/file.hh>

#include "app_config.hh"
#include "external_sort.hh"

static seastar::future<> run_impl(const app_config &config) {
    if (!co_await config.is_valid()) {
        co_return;
    }

    // call the main impl with config
    co_await external_sort(config);

    // remove the temporary directory
    co_await seastar::recursive_remove_directory(
        config.temp_working_dir.c_str());
}

int main(int argc, char **argv) {
    seastar::app_template app;

    app_config::init_flags(app);

    app.run(argc, argv, [&app]() -> seastar::future<> {
        co_await seastar::do_with(app_config(app), run_impl);
    });

    return 0;
}
