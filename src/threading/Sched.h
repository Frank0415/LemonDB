#ifndef PROJECT_SCHED_H
#define PROJECT_SCHED_H

#include <atomic>
#include <functional>
#include <thread>

class Scheduler {
private:
  std::atomic<int> completed_{0};
  std::atomic<int> total_{0};
  std::function<void()> on_complete_;

public:
  /**
   * Setup the scheduler with total tasks and completion callback
   * @param total Number of tasks to track
   * @param callback Function to call when all tasks complete
   */
  void setup(int total, std::function<void()> callback) {
    total_ = total;
    completed_ = 0;
    on_complete_ = std::move(callback);
  }

  /**
   * Notify that a task has completed, triggers callback if all done
   */
  void notifyComplete() {
    if (++completed_ >= total_ && on_complete_) {
      on_complete_();
    }
  }

  /**
   * Wait for all tasks to complete (busy wait with yield)
   */
  void wait() {
    while (completed_ < total_) [[likely]] {
      std::this_thread::yield();
    }
  }

  /**
   * Get the number of remaining tasks
   * @return Number of tasks not yet completed
   */
  [[nodiscard]] int remaining() const { return total_ - completed_; }
};

#endif  // PROJECT_SCHED_H