#pragma once

#include <seastar/core/future.hh>

// forward declaration
class app_config;

// external_sort sorts the given file, that cannot fit in memory, in a
// distributed fashion using the seastar framework.
seastar::future<> external_sort(const app_config &config);
