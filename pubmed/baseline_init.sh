#!/bin/bash

rm -rf config || true
cp -r /config ./

./data-digger-etl/scripts/load-pubmed.sh pubmed-baseline-2-postgres

echo 'Completed!'