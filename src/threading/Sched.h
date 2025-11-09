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
  void setup(int total, std::function<void()> callback) {
    total_ = total;
    completed_ = 0;
    on_complete_ = std::move(callback);
  }

  void notifyComplete() {
    if (++completed_ >= total_ && on_complete_) {
      on_complete_();
    }
  }

  void wait() {
    while (completed_ < total_) [[likely]]
    {
      std::this_thread::yield();
    }
  }

  [[nodiscard]] int remaining() const { return total_ - completed_; }
};

#endif // PROJECT_SCHED_H