#ifndef PROJECT_SCHED_H
#define PROJECT_SCHED_H

#include <atomic>
#include <functional>
#include <thread>
#include <utility>

/**
 * Lightweight task completion tracker with optional completion callback.
 *
 * Usage pattern:
 *  - Call `setup(total, callback)` before dispatching tasks.
 *  - Each task invokes `notifyComplete()` when finished.
 *  - When the number of completed tasks reaches `total`, the callback is
 *    invoked (if provided).
 *  - `wait()` performs a busy-wait loop yielding the CPU until all tasks
 *    report completion.
 *
 * Notes:
 *  - This implementation uses atomics and a spin/yield loop; for large or
 *    unpredictable wait durations consider replacing `wait()` logic with a
 *    condition_variable to avoid wasted CPU cycles.
 *  - `setup` resets internal counters; do not call it concurrently with
 *    in-flight tasks from a previous round.
 */
class Scheduler {
private:
  /**
   * Number of tasks that have completed so far.
   */
  std::atomic<int> completed_{0};
  /**
   * Total number of tasks expected.
   */
  std::atomic<int> total_{0};
  /**
   * Optional callback invoked exactly once when all tasks complete.
   */
  std::function<void()> on_complete_;

public:
  /**
   * Initialize / reset the scheduler with a total task count and optional
   * completion callback.
   * @param total Number of tasks expected (must be >= 0).
   * @param callback Function invoked when all tasks complete (may be empty).
   */
  void setup(int total, std::function<void()> callback) {
    total_ = total;
    completed_ = 0;
    on_complete_ = std::move(callback);
  }

  /**
   * Atomically record completion of one task. If this brings the number
   * of completed tasks to `total_`, invokes the completion callback (if any).
   * Safe to call from multiple threads.
   */
  void notifyComplete() {
    if (++completed_ >= total_ && on_complete_) {
      on_complete_();
    }
  }

  /**
   * Busy-wait (with periodic `std::this_thread::yield()`) until all tasks
   * have completed. Suitable only for short wait durations. For longer
   * waits, prefer adding a blocking primitive.
   */
  void wait() {
    while (completed_ < total_) [[likely]] {
      std::this_thread::yield();
    }
  }

  /**
   * Get the number of tasks not yet completed.
   * @return Remaining task count (may be negative if misused).
   */
  [[nodiscard]] int remaining() const { return total_ - completed_; }
};

#endif  // PROJECT_SCHED_H