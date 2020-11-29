git clean -dxff
go build maplejuice.go mp1.go mp2.go utils.go
go build ./applications/wordFreq_maple.go
go build ./applications/wordFreq_juice.go
mv wordFreq_maple ./applications
mv wordFreq_juice ./applications