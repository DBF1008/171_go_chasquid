#!/bin/bash

set -e
. "$(dirname "$0")/../util/lib.sh"

init
check_hostaliases

# This is an integration test with Prometheus, so skip if it is not present.
if ! prometheus --version > /dev/null; then
	skip "prometheus not installed"
fi
if ! promtool help > /dev/null; then
	skip "promtool not installed"
fi

# Start chasquid and prometheus.
rm -rf .logs/ .prometheus/
mkdir -p .logs .prometheus

generate_certs_for testserver
chasquid-util-user-add user@testserver secretpassword
chasquid -v=2 --logfile=.logs/chasquid.log --config_dir=config &

prometheus \
	--config.file=prometheus.yml \
	--storage.tsdb.path=.prometheus/ \
	> .prometheus/log 2>&1 &


# Wait until they're both up and running.
wait_until_ready 1025
wait_until "promtool check ready 2>/dev/null"

# Send an email.
smtpc user@testserver < content
wait_for_file .mail/user@testserver
mail_diff content .mail/user@testserver


# Query Prometheus and validate that it has scraped chasquid correctly.
function expect_value() {
	promtool query instant http://localhost:9090 "$1" > ".logs/value-$1"
	grep -q "=> $2" ".logs/value-$1"
}

# Note that it takes Prometheus ~5s to start scraping targets on the first
# run, for reasons currently unknown.
# If we don't clear up .prometheus/, then subsequent runs are faster, but
# starting with an empty data directory makes the test more hermetic.
wait_until expect_value chasquid_queue_putCount 1

success
