#ifndef PROJECT_THREADPOOL_H
#define PROJECT_THREADPOOL_H

#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <queue>
#include <thread>
#include <vector>

class ThreadPool
{
private:
  static std::unique_ptr<ThreadPool> global_instance;
  static std::mutex instance_mutex;
  static bool initialized;

  mutable std::mutex lockx;
  mutable std::queue<std::function<void()>> Task_assemble;
  std::vector<std::thread> pool_vector;
  mutable std::condition_variable cv;
  std::atomic<bool> done;
  std::atomic<int> idleThreadNum;
  size_t total_threads;

  // a manager for threads to constantly work until the ThreadPool is destructed
  void thread_manager()
  {
    while (!done.load()) [[likely]]
    {
      std::function<void()> task;
      {
        std::unique_lock<std::mutex> lock(lockx);
        cv.wait(lock, [this]() { return !Task_assemble.empty() || done.load(); });

        if (done.load() && Task_assemble.empty()) [[unlikely]]
        {
          return;
        }

        task = std::move(Task_assemble.front());
        Task_assemble.pop();
      }

      idleThreadNum--;
      task();
      idleThreadNum++;
    }
  }

  // Private constructor for singleton
  explicit ThreadPool(size_t num_threads)
      : done(false), idleThreadNum(0), total_threads(num_threads)
  {
    for (size_t i = 0; i < num_threads; ++i)
    {
      pool_vector.emplace_back(&ThreadPool::thread_manager, this);
      idleThreadNum++;
    }
  }

public:
  // Deleted copy constructor and assignment operator
  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;
  ThreadPool(ThreadPool&&) = delete;
  ThreadPool& operator=(ThreadPool&&) = delete;

  static void initialize(size_t num_threads = std::thread::hardware_concurrency())
  {
    const std::scoped_lock<std::mutex> lock(instance_mutex);
    if (initialized) [[unlikely]]
    {
      return;
    }
    global_instance = std::unique_ptr<ThreadPool>(new ThreadPool(num_threads));
    initialized = true;
  }

  static ThreadPool& getInstance()
  {
    const std::scoped_lock<std::mutex> lock(instance_mutex);
    if (!initialized) [[unlikely]]
    {
      throw std::runtime_error("ThreadPool not initialized. Call initialize() first.");
    }
    return *global_instance;
  }

  static bool isInitialized()
  {
    const std::scoped_lock<std::mutex> lock(instance_mutex);
    return initialized;
  }

  ~ThreadPool()
  {
    done.store(true);
    cv.notify_all();
    for (auto& thread : pool_vector)
    {
      if (thread.joinable())
      {
        thread.join();
      }
    }
  }

  template <typename F, typename... Args>
  auto submit(F&& func, Args&&... args) const -> std::future<decltype(func(args...))>
  {
    using return_type = decltype(func(args...));

    auto task = std::make_shared<std::packaged_task<return_type()>>(
        [func_cap = std::forward<F>(func), ... args_tuple = std::forward<Args>(args)]() mutable
        { return func_cap(std::forward<Args>(args_tuple)...); });

    std::future<return_type> ret = task->get_future();
    {
      const std::scoped_lock<std::mutex> lock(lockx);
      Task_assemble.emplace([task]() { (*task)(); });
    }
    cv.notify_one();
    return ret;
  }

  [[nodiscard]] int getIdleThreadNum() const
  {
    return idleThreadNum.load();
  }
  [[nodiscard]] size_t getThreadCount() const
  {
    return total_threads;
  }
};

#endif // PROJECT_THREADPOOL_H