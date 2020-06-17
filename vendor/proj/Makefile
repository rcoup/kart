PROJ_VERSION ?= 7.0.1
SHELL = /bin/bash

export PREFIX ?= $(abspath env)

ifeq ($(OS),Windows_NT)
	PLATFORM := Windows
else
	PLATFORM := $(shell uname -s)
endif

PROJ_ARCHIVE := proj-$(PROJ_VERSION).tar.gz

export PKG_CONFIG_PATH := $(realpath $(PREFIX)/lib/pkgconfig)
ifeq ($(PLATFORM),Darwin)
	CCACHE_PATH = /usr/local/opt/ccache/bin
	LIBSUFFIX = dylib
	export PKG_CONFIG_PATH += /usr/local/opt/sqlite/lib/pkgconfig
	CURL_CONFIG = /usr/local/opt/curl/bin/curl-config
else ifeq ($(PLATFORM),Linux)
	CCACHE_PATH = /usr/lib/ccache
	CCACHE_PATH := $(or $(CCACHE_PATH),/usr/lib64/ccache)
	CURL_CONFIG := $(PREFIX)/bin/curl-config
	LIBSUFFIX = so
endif

# use ccache if available
export PATH := $(CCACHE_PATH):$(PREFIX)/bin:$(PATH)

export PKG_CONFIG_PATH := $(realpath $(PREFIX)/lib/pkgconfig)

build-proj := src/src/.libs/libproj.$(LIBSUFFIX)
configure-proj = src/config.status

libsqlite := $(PREFIX)/lib/libsqlite3.$(LIBSUFFIX)
libs := $(libsqlite)

# default target
.PHONY: all
all: $(build-proj)

.PHONY: clean
clean:
	-$(RM) -r env
	-$(RM) src/src/.libs/libproj.*
	-$(RM) src/src/libproj.la
	-$(RM) src/proj.pc

.PHONY: cleaner
cleaner: clean
	-$(MAKE) -C src/ clean

.PHONY: cleanest
cleanest:
	-$(RM) -r src/

.PHONY: clean-configure
clean-configure:
	-$(RM) $(configure-proj)

#
# Download Archives
#

$(PROJ_ARCHIVE):
	wget https://download.osgeo.org/proj/$@ -O $@

.PHONY: archive
archive: $(PROJ_ARCHIVE)

#
# Extract Archives
#

src: $(PROJ_ARCHIVE)
	rm -rf $@
	mkdir -p $@
	@echo "Extracting $(PROJ_ARCHIVE) ..."
	tar xzf $(PROJ_ARCHIVE) --strip-components=1 -C $@

.PHONY: source
source: src

#
# PROJ
#

.PHONY: configure
configure: clean-configure $(configure-proj)

$(configure-proj): CFLAGS+=$(SQLITE_CFLAGS)
$(configure-proj): LDFLAGS+=$(SQLITE_LDFLAGS)
$(configure-proj): | src $(libs)
	cd src/ && \
	./configure \
		--prefix=$(PREFIX) \
		--with-curl=$(CURL_CONFIG)

$(build-proj): $(configure-proj)
	$(MAKE) -C src/ -j 2
	$(MAKE) print-lib-deps-$(PLATFORM)

.PHONY: install
install: $(build-proj)
	$(MAKE) -C src/ install
# 	@echo Installing proj-data ...
# 	$(PREFIX)/bin/projsync --system-directory

.PHONY: print-lib-deps
print-lib-deps: print-lib-deps-$(PLATFORM)

.PHONY: print-lib-deps-Darwin
print-lib-deps-Darwin:
	@echo -e "\nExamining library dependencies..."
	find src/ \( -name "*.dylib" -o -name "*.so" \) -exec otool -L {} \;

.PHONY: print-lib-deps-Linux
print-lib-deps-Linux:
	@echo -e "\nExamining library dependencies..."
	find src/ -name "*.so" -print -exec ldd {} \;
