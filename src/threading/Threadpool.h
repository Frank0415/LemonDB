#ifndef PROJECT_THREADPOOL_H
#define PROJECT_THREADPOOL_H

#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <queue>
#include <thread>
#include <vector>

/**
 * A simple fixed-size thread pool singleton for executing submitted tasks.
 *
 * Tasks are stored as `std::function<void()>` in a FIFO queue and picked up
 * by worker threads that block on a condition variable. The pool must be
 * explicitly initialized via `initialize()` before first use; afterwards
 * `getInstance()` returns the global instance. Destruction signals all
 * workers to stop and joins them.
 */
class ThreadPool {
private:
  /**
   * Owning pointer to the global singleton instance.
   */
  static std::unique_ptr<ThreadPool> global_instance;
  /**
   * Mutex guarding initialization and access to the singleton pointer.
   */
  static std::mutex instance_mutex;
  /**
   * Flag indicating whether the pool has been initialized.
   */
  static bool initialized;

  /**
   * Mutex protecting task queue access.
   */
  mutable std::mutex lockx;
  /**
   * FIFO queue holding pending tasks.
   */
  mutable std::queue<std::function<void()>> Task_assemble;
  /**
   * Vector of worker threads.
   */
  std::vector<std::thread> pool_vector;
  /**
   * Condition variable used by workers to wait for new tasks or shutdown.
   */
  mutable std::condition_variable cv;
  /**
   * Atomic flag signaling shutdown to workers.
   */
  std::atomic<bool> done;
  /**
   * Number of currently idle threads (approximate bookkeeping).
   */
  std::atomic<int> idleThreadNum;
  /**
   * Total configured worker thread count.
   */
  size_t total_threads;

  /**
   * Worker thread routine: waits for tasks and executes them until shutdown.
   * Blocks on the condition variable; exits when `done` is true and the task
   * queue is empty.
   */
  void thread_manager() {
    while (!done.load()) [[likely]] {
      std::function<void()> task;
      {
        std::unique_lock<std::mutex> lock(lockx);
        cv.wait(lock,
                [this]() { return !Task_assemble.empty() || done.load(); });

        if (done.load() && Task_assemble.empty()) [[unlikely]] {
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

  /**
   * Private constructor; creates workers and sets initial idle count.
   * @param num_threads Number of worker threads to spawn.
   */
  explicit ThreadPool(size_t num_threads)
      : done(false), idleThreadNum(0), total_threads(num_threads) {
    for (size_t i = 0; i < num_threads; ++i) {
      pool_vector.emplace_back(&ThreadPool::thread_manager, this);
      idleThreadNum++;
    }
  }

public:
  // Deleted copy constructor and assignment operator
  ThreadPool(const ThreadPool &) = delete;
  ThreadPool &operator=(const ThreadPool &) = delete;
  ThreadPool(ThreadPool &&) = delete;
  ThreadPool &operator=(ThreadPool &&) = delete;

  /**
   * Initialize the global thread pool with specified number of threads.
   * Safe to call multiple times; subsequent calls are ignored.
   * @param num_threads Number of worker threads to create (default: hardware
   * concurrency).
   */
  static void
  initialize(size_t num_threads = std::thread::hardware_concurrency()) {
    const std::scoped_lock<std::mutex> lock(instance_mutex);
    if (initialized) [[unlikely]] {
      return;
    }
    global_instance = std::unique_ptr<ThreadPool>(new ThreadPool(num_threads));
    initialized = true;
  }

  /**
   * Get the global thread pool instance.
   * @throws std::runtime_error if called before initialization.
   */
  static ThreadPool &getInstance() {
    const std::scoped_lock<std::mutex> lock(instance_mutex);
    if (!initialized) [[unlikely]] {
      throw std::runtime_error(
          "ThreadPool not initialized. Call initialize() first.");
    }
    return *global_instance;
  }

  /**
   * Check if the thread pool has been initialized.
   */
  static bool isInitialized() {
    const std::scoped_lock<std::mutex> lock(instance_mutex);
    return initialized;
  }

  ~ThreadPool() {
    done.store(true);
    cv.notify_all();
    for (auto &thread : pool_vector) {
      if (thread.joinable()) {
        thread.join();
      }
    }
  }

  /**
   * Submit a callable task to the pool for asynchronous execution.
   * Captures callable and arguments by perfect-forwarding and returns a
   * future for the result.
   * Thread-safe; may block briefly on internal mutex.
   * @tparam F Callable type.
   * @tparam Args Argument types.
   * @param func Callable to invoke.
   * @param args Arguments forwarded to the callable.
   * @return std::future holding the callable's return value.
   */
  template <typename F, typename... Args>
  auto submit(F &&func,
              Args &&...args) const -> std::future<decltype(func(args...))> {
    using return_type = decltype(func(args...));

    auto task = std::make_shared<std::packaged_task<return_type()>>(
        [func_cap = std::forward<F>(func),
         ... args_tuple = std::forward<Args>(args)]() mutable {
          return func_cap(std::forward<Args>(args_tuple)...);
        });

    std::future<return_type> ret = task->get_future();
    {
      const std::scoped_lock<std::mutex> lock(lockx);
      Task_assemble.emplace([task]() { (*task)(); });
    }
    cv.notify_one();
    return ret;
  }

  /**
   * Get the current count of idle worker threads.
   */
  [[nodiscard]] int getIdleThreadNum() const { return idleThreadNum.load(); }

  /**
   * Get the total number of worker threads.
   */
  [[nodiscard]] size_t getThreadCount() const { return total_threads; }
};

#endif  // PROJECT_THREADPOOL_H