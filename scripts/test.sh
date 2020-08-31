#!/usr/bin/env bash

set -e
set -x

pytest --cov=kafka_streamer --cov-report=term-missing tests "${@}"
