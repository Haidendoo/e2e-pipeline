#!/bin/bash
arr=()
while true; do
    arr+=($(head -c 1M </dev/urandom | base64))
    sleep 1
done
