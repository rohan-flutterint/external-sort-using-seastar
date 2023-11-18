#pragma once

#include <seastar/core/seastar.hh>

// forward declaration
namespace seastar {
class app_template;
} // namespace seastar

// struct to hold and pass around the app config
struct app_config {
    std::string input_filename;
    std::string output_filename;
    std::string temp_working_dir;

    // registers the flags to the app template
    static void init_flags(seastar::app_template &app);

    // parses the flags from the given app
    app_config(seastar::app_template &app);

    seastar::future<bool> is_valid() const;

  private:
    // creates a temporary working directory to be used by the sort
    void create_temp_working_dir(std::filesystem::path tempdir);
};
