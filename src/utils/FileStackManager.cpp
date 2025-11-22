#include "FileStackManager.h"
#include <iostream>
#include <stdexcept>

void FileStackManager::pushFile(const std::string &filename) {
  auto file = std::make_unique<std::ifstream>(filename);
  if (!file->is_open()) {
    throw std::runtime_error("Cannot open file: " + filename);
  }
  file_stack.push(std::move(file));
  file_path_stack.push(filename);
}

void FileStackManager::popFile() {
  if (!file_stack.empty()) {
    file_stack.pop();
    file_path_stack.pop();
  }
}

std::ifstream *FileStackManager::getCurrentStream() {
  if (file_stack.empty()) {
    return nullptr;
  }
  return file_stack.top().get();
}

std::string FileStackManager::getCurrentFilePath() const {
  if (file_path_stack.empty()) {
    return "";
  }
  return file_path_stack.top();
}

bool FileStackManager::isEmpty() const { return file_stack.empty(); }

std::string FileStackManager::resolvePath(const std::string &filename) const {
  if (filename.empty()) {
    return "";
  }

  // Handle absolute paths
  if (filename[0] == '/') {
    return filename;
  }

  // No context available
  if (file_path_stack.empty()) {
    return filename;
  }

  // Relative to current file's directory
  std::string current = file_path_stack.top();
  size_t last_slash = current.rfind('/');
  if (last_slash == std::string::npos) {
    return filename;
  }

  return current.substr(0, last_slash + 1) + filename;
}
