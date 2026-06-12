#!/bin/sh

# Hook that returns addresses designed to overlap with alias file entries
# in the mixed scenario test. It returns "shared@remote" (which overlaps
# with an alias entry) and "hook-only@remote" (which is unique to the hook).
echo "shared@remote, hook-only@remote"
