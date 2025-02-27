# 构建参数
BINARY_NAME=dbtransfer
VERSION=1.0.0
BUILD_DIR=build
MAIN_FILE=main.go

# Go 命令
GO=go
GOBUILD=$(GO) build
GOTEST=$(GO) test
GOCLEAN=$(GO) clean
GOMOD=$(GO) mod
GOVET=$(GO) vet
GOFMT=gofmt

# 编译标记
LDFLAGS=-ldflags "-X main.Version=${VERSION} -s -w"

# 操作系统和架构
PLATFORMS=linux darwin windows
ARCHITECTURES=amd64 arm64

.PHONY: all build clean test fmt vet tidy vendor build-all help linux-amd64 linux-arm64 darwin-amd64 darwin-arm64

# 默认目标
all: clean build

# 构建当前平台的二进制文件
build:
	@echo "Building ${BINARY_NAME}..."
	@mkdir -p ${BUILD_DIR}
	$(GOBUILD) ${LDFLAGS} -o ${BUILD_DIR}/${BINARY_NAME} ${MAIN_FILE}

# 清理构建文件
clean:
	@echo "Cleaning..."
	@rm -rf ${BUILD_DIR}
	$(GOCLEAN)

# 运行测试
test:
	@echo "Running tests..."
	$(GOTEST) -v ./...

# 格式化代码
fmt:
	@echo "Formatting code..."
	$(GOFMT) -w .

# 运行 go vet
vet:
	@echo "Running go vet..."
	$(GOVET) ./...

# 更新依赖
tidy:
	@echo "Tidying dependencies..."
	$(GOMOD) tidy

# 下载依赖到 vendor 目录
vendor:
	@echo "Vendoring dependencies..."
	$(GOMOD) vendor

# 构建所有平台的二进制文件
build-all:
	@echo "Building for all platforms..."
	@mkdir -p ${BUILD_DIR}
	@for platform in ${PLATFORMS}; do \
		for arch in ${ARCHITECTURES}; do \
			output_name=${BUILD_DIR}/${BINARY_NAME}-$${platform}-$${arch}; \
			if [ "$${platform}" = "windows" ]; then \
				output_name=$${output_name}.exe; \
			fi; \
			echo "Building $${platform}/$${arch}..."; \
			GOOS=$${platform} GOARCH=$${arch} $(GOBUILD) ${LDFLAGS} -o $${output_name} ${MAIN_FILE}; \
		done; \
	done

# 构建特定平台的二进制文件
linux-amd64:
	@echo "Building for Linux AMD64..."
	@mkdir -p ${BUILD_DIR}
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GOBUILD) ${LDFLAGS} -o ${BUILD_DIR}/${BINARY_NAME}.linux.x86_64 ${MAIN_FILE}

linux-arm64:
	@echo "Building for Linux ARM64..."
	@mkdir -p ${BUILD_DIR}
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 $(GOBUILD) ${LDFLAGS} -o ${BUILD_DIR}/${BINARY_NAME}.linux.aarch64 ${MAIN_FILE}

darwin-amd64:
	@echo "Building for macOS AMD64..."
	@mkdir -p ${BUILD_DIR}
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 $(GOBUILD) ${LDFLAGS} -o ${BUILD_DIR}/${BINARY_NAME}.darwin.amd64 ${MAIN_FILE}

darwin-arm64:
	@echo "Building for macOS ARM64..."
	@mkdir -p ${BUILD_DIR}
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 $(GOBUILD) ${LDFLAGS} -o ${BUILD_DIR}/${BINARY_NAME}.darwin.arm64 ${MAIN_FILE}

# 显示帮助信息
help:
	@echo "Available commands:"
	@echo "  make linux-amd64  - Build binary for Linux AMD64"
	@echo "  make linux-arm64  - Build binary for Linux ARM64"
	@echo "  make darwin-amd64 - Build binary for macOS AMD64"
	@echo "  make darwin-arm64 - Build binary for macOS ARM64"
	@echo "  make          - Build binary for current platform"
	@echo "  make build    - Same as 'make'"
	@echo "  make clean    - Remove build directory and clean Go cache"
	@echo "  make test     - Run tests"
	@echo "  make fmt      - Format code"
	@echo "  make vet      - Run go vet"
	@echo "  make tidy     - Tidy Go modules"
	@echo "  make vendor   - Vendor dependencies"
	@echo "  make build-all- Build binaries for all platforms"
	@echo "  make help     - Show this help message"
