#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

BENCHMARK_PROPERTIES_FILE=${1:-driver/benchmark.properties}

# Required for Java 9+: LDBC driver uses legacy SBE library (1.0.3-RC2) that accesses internal sun.nio.ch.DirectBuffer API.
# Without this flag, Java 21 module system will block access with IllegalAccessError.
java --add-exports java.base/sun.nio.ch=ALL-UNNAMED -cp ../runner/target/runner-1.0-SNAPSHOT.jar org.ldbcouncil.snb.driver.Client -P ${BENCHMARK_PROPERTIES_FILE}