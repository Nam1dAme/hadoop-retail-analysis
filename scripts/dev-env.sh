#!/usr/bin/env bash
# 必须 source；全部路径限定本项目，不加载旧项目模块。
_behavior_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
export BEHAVIOR_ROOT="$_behavior_root"
export JAVA_HOME="$BEHAVIOR_ROOT/var/tools/java11"
export HADOOP_HOME="$BEHAVIOR_ROOT/var/tools/hadoop-3.4.2"
export HADOOP_CONF_DIR="$BEHAVIOR_ROOT/config/hadoop"
export HADOOP_COMMON_HOME="$HADOOP_HOME" HADOOP_HDFS_HOME="$HADOOP_HOME"
export HADOOP_YARN_HOME="$HADOOP_HOME" HADOOP_MAPRED_HOME="$HADOOP_HOME"
export HADOOP_LOG_DIR="$BEHAVIOR_ROOT/var/log/hadoop"
export HADOOP_PID_DIR="$BEHAVIOR_ROOT/var/run/hadoop"
export HADOOP_IDENT_STRING=behavior
export HADOOP_STOP_TIMEOUT=30
export OMP_NUM_THREADS=4 MKL_NUM_THREADS=4
export PATH="$BEHAVIOR_ROOT/.venv/bin:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
unset _behavior_root
