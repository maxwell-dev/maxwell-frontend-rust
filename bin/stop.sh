#!/usr/bin/env bash

current_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )";
cd ${current_dir}

kill `ps aux | grep candlebase-server | grep -v grep | awk '{print $2}'`