#!/bin/bash

git clean -dxff
go build maplejuice.go mp1.go mp2.go utils.go
go build ./applications/wordFreq_maple.go
go build ./applications/wordFreq_juice.go
go build ./applications/wordCount_maple.go
go build ./applications/wordCount_juice.go
go build ./applications/hw1_maple1.go
go build ./applications/hw1_juice1.go
go build ./applications/hw1_juice2.go
mv wordFreq_maple ./applications
mv wordFreq_juice ./applications
mv wordCount_maple ./applications
mv wordCount_juice ./applications
mv hw1_maple1 ./applications
mv hw1_juice1 ./applications
mv hw1_juice2 ./applications

if [ "$HOSTNAME" = fa20-cs425-g35-01.cs.illinois.edu ];
then
    go build generateInput.go
    ./generateInput
fi

if [ "$HOSTNAME" = fa20-cs425-g35-01 ];
then
    go build generateInput.go
    ./generateInput
fi