#ifndef LEMONDB_FILESTACKMANAGER_H
#define LEMONDB_FILESTACKMANAGER_H

#include <fstream>
#include <memory>
#include <stack>
#include <string>

/**
 * Manager maintaining a stack of input file streams with their paths.
 *
 * Supports nested includes or LISTEN-like functionality where one file
 * can push another file to be processed, then resume the previous one
 * after completion. Paths are stored alongside streams to allow relative
 * path resolution for subsequently pushed files.
 */
class FileStackManager {
private:
  /**
   * Stack of owning pointers to open input file streams (top is current).
   */
  std::stack<std::unique_ptr<std::ifstream>> file_stack;
  /**
   * Stack of file paths corresponding to each stream (kept in sync with
   * file_stack).
   */
  std::stack<std::string> file_path_stack;

public:
  /**
   * Default constructor; creates an empty manager.
   */
  FileStackManager() = default;

  /**
   * Open a file and push its stream + resolved path onto the stack.
   * If a current file exists and the provided filename is relative,
   * it is resolved against the directory of the current file.
   * @param filename File name (absolute or relative) to open.
   * Throws if the file cannot be opened.
   */
  void pushFile(const std::string &filename);

  /**
   * Pop (close) the current file stream and restore the previous one.
   * Safe to call when stack has exactly one element (result becomes empty).
   * No-op if stack is already empty.
   */
  void popFile();

  /**
   * Get pointer to the current (top) input stream.
   * @return Pointer to std::ifstream or nullptr if empty.
   */
  std::ifstream *getCurrentStream();

  /**
   * Get the path of the current (top) file.
   * @return File path string or empty string if stack is empty.
   */
  std::string getCurrentFilePath() const;

  /**
   * Check whether there are no active file streams.
   * @return true if stack is empty, false otherwise.
   */
  bool isEmpty() const;

  /**
   * Resolve a file name to an absolute / canonical path for opening.
   * If there is a current file and filename is relative, it is joined
   * with the current file's directory. Otherwise returns filename unchanged.
   * @param filename Raw file name provided by caller (relative or absolute).
   * @return Resolved path string suitable for std::ifstream opening.
   */
  std::string resolvePath(const std::string &filename) const;
};

#endif
