#!/usr/bin/env bash

current_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )";
cd ${current_dir}

nohup releases/current/maxwell-frontend >/dev/null 2>&1 &