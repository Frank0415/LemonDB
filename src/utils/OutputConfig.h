#ifndef PROJECT_OUTPUT_CONFIG_H
#define PROJECT_OUTPUT_CONFIG_H

#include <chrono>
#include <cstddef>

struct OutputConfig {
  static constexpr size_t kDefaultThreshold1 = 100;
  static constexpr size_t kDefaultThreshold2 = 1000;
  static constexpr size_t kDefaultThreshold3 = 10000;

  static constexpr std::chrono::milliseconds kDefaultInterval1{10};
  static constexpr std::chrono::milliseconds kDefaultInterval2{50};
  static constexpr std::chrono::milliseconds kDefaultInterval3{100};
  static constexpr std::chrono::milliseconds kDefaultInterval4{200};

  size_t threshold1 = kDefaultThreshold1;
  size_t threshold2 = kDefaultThreshold2;
  size_t threshold3 = kDefaultThreshold3;

  std::chrono::milliseconds interval1{kDefaultInterval1};
  std::chrono::milliseconds interval2{kDefaultInterval2};
  std::chrono::milliseconds interval3{kDefaultInterval3};
  std::chrono::milliseconds interval4{kDefaultInterval4};
};

[[nodiscard]] std::chrono::
    milliseconds
    /**
     * Calculate adaptive output interval based on total output count
     * @param total_output_count Number of output items processed
     * @param config Output configuration with thresholds and intervals
     * @return Appropriate interval duration for output flushing
     */
    calculateOutputInterval(size_t total_output_count,
                            const OutputConfig &config = OutputConfig{});

#endif  // PROJECT_OUTPUT_CONFIG_H
