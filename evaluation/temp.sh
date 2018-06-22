#!/bin/bash
while IFS=',' read -ra line || [[ -n "$line" ]]; do
	echo "${line[10]}"
done < "$1"