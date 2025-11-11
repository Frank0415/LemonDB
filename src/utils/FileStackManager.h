#ifndef LEMONDB_FILESTACKMANAGER_H
#define LEMONDB_FILESTACKMANAGER_H

#include <memory>
#include <stack>
#include <string>
#include <fstream>

class FileStackManager {
private:
  std::stack<std::unique_ptr<std::ifstream>> file_stack;
  std::stack<std::string> file_path_stack;

public:
  FileStackManager() = default;

  void pushFile(const std::string& filename);

  void popFile();

  std::ifstream* getCurrentStream();

  std::string getCurrentFilePath() const;

  bool isEmpty() const;

  std::string resolvePath(const std::string& filename) const;
};

#endif
