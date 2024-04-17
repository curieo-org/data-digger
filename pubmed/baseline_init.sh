#!/bin/bash

rm -f config || true
mv /config/ config

./data-digger-etl/scripts/load-pubmed.sh pubmed-baseline-2-postgres

echo 'Completed!'