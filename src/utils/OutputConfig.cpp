#include "OutputConfig.h"

#include <chrono>
#include <cstddef>

std::chrono::milliseconds calculateOutputInterval(size_t total_output_count,
                                                  const OutputConfig &config) {
  if (total_output_count < config.threshold1) {
    return config.interval1;
  }
  if (total_output_count < config.threshold2) {
    return config.interval2;
  }
  if (total_output_count < config.threshold3) {
    return config.interval3;
  }
  return config.interval4;
}
