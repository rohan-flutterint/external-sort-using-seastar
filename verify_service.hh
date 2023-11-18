#pragma once

#include <exception>

#include "first_pass_service.hh"

// Service that verifies that the entries in the offsets it is responsible are
// lexicographically sorted
class verify_service : public seastar::sharded<verify_service>,
                       private first_pass_service {
  public:
    verify_service(const seastar::file_handle output_file_handle)
        : first_pass_service(output_file_handle, "") {}

    seastar::future<> run();
    seastar::future<> stop() { return first_pass_service::stop(); }
};

class verification_exception : public std::exception {
  private:
    std::string message;

  public:
    verification_exception(const char *msg) : message(msg) {}
    const char *what() { return message.c_str(); }
};
