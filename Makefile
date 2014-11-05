dev:
	go build

deps:
	go get -d

linux:
	GOARCH=amd64 GOOS=linux go build

clean:
	go clean
