#include "Threadpool.h"
#include <memory>
#include <mutex>

std::unique_ptr<ThreadPool> ThreadPool::global_instance = nullptr;
std::mutex ThreadPool::instance_mutex;
bool ThreadPool::initialized = false;
