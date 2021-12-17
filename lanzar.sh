#!/bin/bash

echo "Lanzando servidor 0"
go run cmd/srvraft/main.go 0 127.0.0.1:20050 127.0.0.1:20051 127.0.0.1:20052 &
echo "Lanzando servidor 1"
go run cmd/srvraft/main.go 1 127.0.0.1:20050 127.0.0.1:20051 127.0.0.1:20052 &
echo "Lanzando servidor 2"
go run cmd/srvraft/main.go 2 127.0.0.1:20050 127.0.0.1:20051 127.0.0.1:20052 &