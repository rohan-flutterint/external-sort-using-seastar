#include "app_config.hh"

#include <filesystem>

#include <seastar/core/app-template.hh>
#include <seastar/util/file.hh>

#include "common.hh"

void app_config::init_flags(seastar::app_template &app) {
    app.add_options()
        // filename arg to read the name of the file that needs to be sorted
        ("input-filename,f",
         boost::program_options::value<std::string>()->required(),
         "Path to the file that has the records.")
        // temp directory to store the intermediate files
        ("tempdir,t",
         boost::program_options::value<std::filesystem::path>()->default_value(
             {}),
         "Path to the temp directory to store intermediate files.")
        // destination directory for the output file
        ("output-dir,o",
         boost::program_options::value<std::filesystem::path>()->default_value(
             {}),
         "Directory to store the sorted result file. By default, the result "
         "will be stored in the same directory as the input data.");
}

app_config::app_config(seastar::app_template &app) {
    auto &args = app.configuration();

    // extract the arguments, filling with default values wherever necessary
    input_filename = args["input-filename"].as<std::string>();

    create_temp_working_dir(args["tempdir"].as<std::filesystem::path>());

    auto output_dir = args["output-dir"].as<std::filesystem::path>();
    if (output_dir.empty()) {
        output_filename = input_filename + ".sorted";
    } else {
        output_filename =
            output_dir /
            std::filesystem::path{input_filename}.filename().concat(".sorted");
    }
}

seastar::future<bool> app_config::is_valid() const {
    if (temp_working_dir.empty()) {
        co_return false;
    }

    if (!co_await seastar::file_exists(input_filename)) {
        logger.error("input file '{}' doesn't exist", input_filename);
        co_return false;
    }

    co_return true;
}

void app_config::create_temp_working_dir(std::filesystem::path tempdir) {
    // create temp directory for intermediate files
    std::error_code ec;
    if (tempdir.empty()) {
        tempdir = std::filesystem::temp_directory_path(ec);
        if (ec.value() != 0) {
            logger.error("failed to get the temporary directory");
            return;
        }
    } else if (!std::filesystem::is_directory(tempdir, ec) || ec.value() != 0) {
        logger.error("tempdir '{}' doesn't exist or is not accesible");
        return;
    }

    tempdir /= "XXXXXX";
    char tempdir_template[tempdir.native().length()];
    strcpy(tempdir_template, tempdir.c_str());
    assert(!tempdir.empty());
    const char *tempdir_path = mkdtemp(tempdir_template);
    if (tempdir_path == nullptr) {
        logger.error("failed to create temporary working directory : {}",
                     strerror(errno));
        return;
    }

    temp_working_dir = tempdir_path;
    logger.info("using '{}' as the temp directory", temp_working_dir.c_str());
}
