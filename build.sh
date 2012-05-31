#!/bin/bash

APR_DIR=/usr/
STOMP_DIR=/usr/local
UUID_DIR=/usr

gcc -std=c99 -L$APR_DIR/lib -L$STOMP_DIR/lib -L$UUID_DIR/lib -I$APR_DIR/include -I$STOMP_DIR/include -I$UUID_DIR/include/uuid -lapr-1 -lstomp -luuid svc-rrd-native.c -o svc-rrd-native

