#!/bin/sh
# Test hook used by TestDedupHook.
#
# It returns recipients only for specific addresses, so we can exercise the
# de-duplication of "aliases file + hook" results, including via the catch-all
# path (where the hook is invoked with the literal "*@domain" address).
case "$1" in
	selfdup@*)
		# The hook itself returns the same recipient twice.
		echo "sd@remote, sd@remote"
		;;
	aliasdup@*)
		# Collides with an entry in the aliases file for this address.
		echo "ad@remote"
		;;
	'*@'*)
		# Collides with the catch-all entry in the aliases file.
		echo "shared@remote"
		;;
esac
