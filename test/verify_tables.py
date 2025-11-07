#!/usr/bin/env python3

import sys
from collections import Counter

def verify_tables(tmp_file, dump_file):
    """
    Compare two table files by checking if they contain the same lines
    (independent of order, since tables are unordered)
    """
    try:
        # Read both files and store as multisets (to handle duplicates)
        with open(tmp_file, 'r') as f:
            tmp_lines = [line.rstrip('\n') for line in f]
        
        with open(dump_file, 'r') as f:
            dump_lines = [line.rstrip('\n') for line in f]
        
        # Check line counts
        if len(tmp_lines) != len(dump_lines):
            print(f"Line count mismatch: tmp={len(tmp_lines)}, dump={len(dump_lines)}", file=sys.stderr)
            return 1
        
        # Check character counts
        tmp_chars = sum(len(line) for line in tmp_lines)
        dump_chars = sum(len(line) for line in dump_lines)
        if tmp_chars != dump_chars:
            print(f"Character count mismatch: tmp={tmp_chars}, dump={dump_chars}", file=sys.stderr)
            return 1
        
        # Compare as multisets (order-independent)
        tmp_counter = Counter(tmp_lines)
        dump_counter = Counter(dump_lines)
        
        if tmp_counter == dump_counter:
            return 0
        else:
            # Show which lines differ
            for line in tmp_counter:
                if tmp_counter[line] != dump_counter.get(line, 0):
                    print(f"Line count mismatch for: {line[:50]}...", file=sys.stderr)
                    return 1
            return 1
    
    except Exception as e:
        print(f"Error comparing files: {e}", file=sys.stderr)
        return 1

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: verify_tables.py <tmp_file> <dump_file> [--verbose]", file=sys.stderr)
        sys.exit(1)
    
    tmp_file = sys.argv[1]
    dump_file = sys.argv[2]
    verbose = len(sys.argv) > 3 and sys.argv[3] == '--verbose'
    
    if verbose:
        print(f"Comparing: {tmp_file} vs {dump_file}")
    
    sys.exit(verify_tables(tmp_file, dump_file))
