#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/dev-env.sh"
cd "$BEHAVIOR_ROOT"
scripts/build-jobs.sh
mkdir -p jobs/target/test-classes
javac --release 11 -cp "jobs/target/behavior-jobs.jar:$(hadoop classpath --glob)" -d jobs/target/test-classes jobs/src/test/java/cn/behavior/*.java
java -Xmx64m -cp "jobs/target/test-classes:jobs/target/behavior-jobs.jar:$(hadoop classpath --glob)" cn.behavior.ContractTest
