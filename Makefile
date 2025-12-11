all: 4730kvstore

4730kvstore: 4730kvstore.go
	go build -o 4730kvstore 4730kvstore.go

clean:
	rm -f 4730kvstore