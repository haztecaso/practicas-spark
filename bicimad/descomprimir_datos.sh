#!/bin/sh
ls ./*.zip | xargs -n 1 -P 8 unzip
