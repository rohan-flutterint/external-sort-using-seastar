#include <filesystem>
#include <iostream>
#include <stdexcept>

#include <seastar/core/app-template.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/file.hh>

#include "common.hh"
#include "external_sort.hh"

seastar::sstring create_temp_directory(std::filesystem::path tempdir) {
    // create temp directory for intermediate files
    std::error_code ec;
    if (tempdir.empty()) {
        tempdir = std::filesystem::temp_directory_path(ec);
        if (ec.value() != 0) {
            logger.error("failed to get temporary directory");
            return {};
        }
    } else if (!std::filesystem::is_directory(tempdir, ec) || ec.value() != 0) {
        logger.error("tempdir '{}' doesn't exist or is not accesible");
        return {};
    }

    tempdir /= "XXXXXX";
    char tempdir_template[tempdir.native().length()];
    strcpy(tempdir_template, tempdir.c_str());
    const char *tempdir_path = mkdtemp(tempdir_template);
    if (tempdir_path == nullptr) {
        logger.error("failed to create temporary directory : {}",
                     strerror(errno));
        return {};
    }

    return seastar::sstring(tempdir_path);
}

int main(int argc, char **argv) {
    seastar::app_template app;

    app.add_options()
        // filename arg to read the name of the file that needs to be sorted
        ("filename,f",
         boost::program_options::value<seastar::sstring>()->required(),
         "path to the file that has the records")
        // temp directory to store the intermediate files
        ("tempdir,t",
         boost::program_options::value<std::filesystem::path>()->default_value(
             {}),
         "path to the temp directory to store intermediate files");

    app.run(argc, argv, [&app] -> seastar::future<> {
        // extract filename and call external_sort on it
        auto &args = app.configuration();
        auto &filename = args["filename"].as<seastar::sstring>();
        auto tempdir =
            create_temp_directory(args["tempdir"].as<std::filesystem::path>());
        if (tempdir.empty()) {
            // failed to create the temp dir
            return seastar::make_ready_future<>();
        }

        logger.info("using '{}' as the temp directory", tempdir);

        return seastar::file_exists(filename).then(
            [filename, tempdir](bool exists) {
                if (!exists) {
                    logger.error("input file '{}' doesn't exist", filename);
                    return seastar::make_ready_future<>();
                }

                return external_sort(filename, tempdir).then([tempdir] {
                    // once sort is complete, clear all the intermediate files
                    return seastar::recursive_remove_directory(tempdir.c_str());
                });
            });
    });

    return 0;
}
