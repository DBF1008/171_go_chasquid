#!/bin/sh

# Hook that returns a fixed address that overlaps with alias file entries.
# Used by TestDeduplication to verify that hook + alias duplicates are removed.
echo "c@d"
