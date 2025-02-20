build:
	mkdir -p ../gobin
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64  go build -a -ldflags '-s -w' -gcflags="all=-trimpath=${PWD}" -asmflags="all=-trimpath=${PWD}" -o ../gobin/dbtransfer.darwin.arm64
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64  go build -a -ldflags '-s -w' -gcflags="all=-trimpath=${PWD}" -asmflags="all=-trimpath=${PWD}" -o ../gobin/dbtransfer.darwin.amd64
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64  go build -a -ldflags '-s -w' -gcflags="all=-trimpath=${PWD}" -asmflags="all=-trimpath=${PWD}" -o ../gobin/dbtransfer.linux.aarch64
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64  go build -a -ldflags '-s -w' -gcflags="all=-trimpath=${PWD}" -asmflags="all=-trimpath=${PWD}" -o ../gobin/dbtransfer.linux.x86_64
	cp ../gobin/dbtransfer.linux.x86_64 ../gobin/dbtransfer
amd64:
	mkdir -p ../gobin
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64  go build -a -ldflags '-s -w' -gcflags="all=-trimpath=${PWD}" -asmflags="all=-trimpath=${PWD}" -o ../gobin/dbtransfer.linux.x86_64
	cp ../gobin/dbtransfer.linux.x86_64 ../gobin/dbtransfer
aarch64:
	mkdir -p ../gobin
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -a -ldflags '-s -w' -gcflags="all=-trimpath=${PWD}" -asmflags="all=-trimpath=${PWD}" -o ../gobin/dbtransfer.linux.aarch64
darwin:
	mkdir -p ../gobin
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64  go build -a -ldflags '-s -w' -gcflags="all=-trimpath=${PWD}" -asmflags="all=-trimpath=${PWD}" -o ../gobin/dbtransfer.darwin.arm64
darwinamd64:
	mkdir -p ../gobin
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64  go build -a -ldflags '-s -w' -gcflags="all=-trimpath=${PWD}" -asmflags="all=-trimpath=${PWD}" -o ../gobin/dbtransfer.darwin.amd64
