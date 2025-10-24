#ifndef PROJECT_THREADPOOL_H
#define PROJECT_THREADPOOL_H

#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <queue>
#include <thread>
#include <vector>

class ThreadPool {
private:
  std::mutex lockx;
  std::queue<std::function<void()>> Task_assemble;
  std::vector<std::thread> pool_vector;
  std::condition_variable cv;
  std::atomic<bool> done;
  std::atomic<int> idleThreadNum;
public:
  ThreadPool();
  ~ThreadPool();
};

#endif