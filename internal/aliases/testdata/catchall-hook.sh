#!/bin/sh

# Hook that only returns an address for catch-all lookups (addresses
# starting with "*@"). For regular addresses it returns nothing.
# This allows testing catch-all alias + hook overlap: the hook fires
# during lookup("*@dom") but not during lookup("x@dom").
case "$1" in
    \*@*) echo "catch@remote" ;;
    *) exit 0 ;;
esac
