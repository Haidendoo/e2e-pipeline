#!/bin/bash
while true; do
    echo "scale=5000; a(1)*4" | bc -l > /dev/null
done
