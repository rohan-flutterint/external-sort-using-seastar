#pragma once

#include <seastar/core/future.hh>

seastar::future<> external_sort(const seastar::sstring filename,
                                const seastar::sstring tempdir);
