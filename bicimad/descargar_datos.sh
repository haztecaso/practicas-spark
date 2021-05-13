#!/bin/sh
baseurl="https://opendata.emtmadrid.es"
url=$baseurl"/Datos-estaticos/Datos-generales-(1)"
curl -s $url | grep -o '/getattachment/[^"]*' | awk -v u="$baseurl" '{ print u$1 }' | xargs -n 1 -P 8 axel -q
