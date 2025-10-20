#!/bin/bash
# Install git hooks for this project
# Run this script after cloning the repository

set -e

HOOKS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GIT_HOOKS_DIR="$(git rev-parse --git-dir)/hooks"

echo "Installing git hooks..."
echo ""

# Copy hooks to .git/hooks directory
for hook_file in "$HOOKS_DIR"/*; do
    # Skip the install script itself
    if [[ "$(basename "$hook_file")" == "install-hooks.sh" ]]; then
        continue
    fi
    
    hook_name=$(basename "$hook_file")
    hook_destination="$GIT_HOOKS_DIR/$hook_name"
    
    cp "$hook_file" "$hook_destination"
    chmod +x "$hook_destination"
    echo "Installed $hook_name"
done

echo ""
echo "Git hooks installed successfully!"
echo ""
echo "Hooks installed:"
echo "commit-msg: Validates commit message format and detects non-ASCII characters"
echo "pre-commit: Prevents non-ASCII characters in committed files"
echo ""
echo "To reinstall hooks, run: ./hooks/install-hooks.sh"
