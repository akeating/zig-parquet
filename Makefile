.DEFAULT_GOAL := help
MAKEFLAGS += --no-print-directory

LIB_DIR := zig-parquet
CLI_DIR := cli
EXAMPLES_DIR := $(LIB_DIR)/examples

# Parameterize builds: `make lib CODECS=c-brotli OPT=ReleaseSmall`
CODECS ?=
OPT ?=
NAME ?=
CODEC_FLAG := $(if $(CODECS),-Dcodecs=$(CODECS))
OPT_FLAG := $(if $(OPT),-Doptimize=$(OPT))

.PHONY: help
help:
	@echo "Run from repo root — never cd into subdirs."
	@echo ""
	@echo "Targets (all accept CODECS=<spec> OPT=<ReleaseSmall|ReleaseFast|...>):"
	@echo "  make build             Build library + CLI"
	@echo "  make lib               Build library (honors CODECS, OPT)"
	@echo "  make cli               Build pqi CLI (honors OPT)"
	@echo "  make test              Library + CLI tests (honors CODECS, OPT)"
	@echo "  make test-lib          Library tests only (honors CODECS, OPT)"
	@echo "  make test-cli          CLI tests only (honors OPT)"
	@echo "  make example NAME=...  Build an example (honors OPT)"
	@echo "  make wasm              Verify WASM targets compile and link"
	@echo "  make validate-wild     Validate pqi against wild files (failures only)"
	@echo "  make validate-wild-all Validate pqi against wild files (show all)"
	@echo "  make fmt               zig fmt across sources"
	@echo "  make clean             Remove .zig-cache and zig-out everywhere"
	@echo ""
	@echo "Examples:"
	@echo "  make test CODECS=zig-only"
	@echo "  make test CODECS=c-zstd,zstd"
	@echo "  make lib CODECS=c-brotli OPT=ReleaseSmall"
	@echo "  make lib CODECS=none OPT=ReleaseSmall"
	@echo "  make example NAME=basic"
	@echo "  make example NAME=wasm_demo OPT=ReleaseSmall"
	@echo ""
	@echo "Examples (NAME=): basic, gen-eeg, gen-power-grid, large_file,"
	@echo "                  wasm_demo, wasm_freestanding"
	@echo ""
	@echo "CODECS values: all (default), none, zig-only, c-only,"
	@echo "  or comma list of: zstd|snappy|gzip|lz4|brotli (Zig)"
	@echo "                    c-zstd|c-snappy|c-gzip|c-lz4|c-brotli (C/C++)"

.PHONY: build
build: lib cli

.PHONY: lib
lib:
	cd $(LIB_DIR) && zig build $(CODEC_FLAG) $(OPT_FLAG)

.PHONY: cli
cli:
	cd $(CLI_DIR) && zig build $(OPT_FLAG)

.PHONY: test
test: test-lib test-cli

.PHONY: test-lib
test-lib:
	cd $(LIB_DIR) && zig build test $(CODEC_FLAG) $(OPT_FLAG)

.PHONY: test-cli
test-cli:
	cd $(CLI_DIR) && zig build test $(OPT_FLAG)

.PHONY: example
example:
	@test -n "$(NAME)" || { echo "usage: make example NAME=<basic|gen-eeg|gen-power-grid|large_file|wasm_demo|wasm_freestanding>"; exit 1; }
	@test -d $(EXAMPLES_DIR)/$(NAME) || { echo "no such example: $(NAME)"; ls $(EXAMPLES_DIR); exit 1; }
	cd $(EXAMPLES_DIR)/$(NAME) && zig build $(OPT_FLAG)

.PHONY: wasm
wasm:
	cd $(LIB_DIR) && zig build wasm-smoke

.PHONY: validate-wild
validate-wild: cli
	cd $(CLI_DIR) && ./validate-wild.sh

.PHONY: validate-wild-all
validate-wild-all: cli
	cd $(CLI_DIR) && ./validate-wild.sh --all

.PHONY: fmt
fmt:
	zig fmt $(LIB_DIR)/src $(LIB_DIR)/build.zig $(CLI_DIR)/src $(CLI_DIR)/build.zig

.PHONY: clean
clean:
	rm -rf $(LIB_DIR)/.zig-cache $(LIB_DIR)/zig-out
	rm -rf $(CLI_DIR)/.zig-cache $(CLI_DIR)/zig-out
	rm -rf $(EXAMPLES_DIR)/*/.zig-cache $(EXAMPLES_DIR)/*/zig-out
