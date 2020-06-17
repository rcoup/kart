TIFF_VERSION = 4.1.0

SHELL = /bin/bash
export PREFIX ?= $(abspath env)

ifeq ($(OS),Windows_NT)
	PLATFORM := Windows
else
	PLATFORM := $(shell uname -s)
endif

TIFF_ARCHIVE := tiff-$(TIFF_VERSION).tar.gz

ifeq ($(PLATFORM),Darwin)
	CCACHE_PATH = /usr/local/opt/ccache/bin
	LIBSUFFIX = dylib
else ifeq ($(PLATFORM),Linux)
	LIBSUFFIX = so
	CCACHE_PATH = /usr/lib/ccache
	CCACHE_PATH := $(or $(CCACHE_PATH),/usr/lib64/ccache)
endif


# use ccache if available
export PATH := $(CCACHE_PATH):$(PREFIX)/bin:$(PATH)

build-tiff := src/.libs/libjson-c.4.$(LIBSUFFIX)
configure-tiff = src/config.status

# default target
.PHONY: all
all: $(build-tiff)

.PHONY: clean
clean:
	-$(RM) -r env
	-$(RM) src/.libs/libjson-c.* src/libjson-c.la
	-$(RM) src/json-c*.pc

.PHONY: cleaner
cleaner: clean
	-$(MAKE) -C src/ clean

.PHONY: cleanest
cleanest:
	-$(RM) -r src/

.PHONY: clean-configure
clean-configure:
	-$(RM) $(configure-tiff)

#
# Download Archives
#

$(TIFF_ARCHIVE):
	wget https://download.osgeo.org/libtiff/$@ -O $@

.PHONY: archive
archive: $(TIFF_ARCHIVE)

#
# Extract Archives
#

src: $(TIFF_ARCHIVE)
	rm -rf $@
	mkdir -p $@
	@echo "Extracting $(TIFF_ARCHIVE) ..."
	tar xzf $(TIFF_ARCHIVE) --strip-components=1 -C $@


.PHONY: source
source: src

$(configure-tiff): export PKG_CONFIG_PATH=/dev/null
$(configure-tiff): | src
	cd src/ \
	&& ./configure --prefix=$(PREFIX) \
		--with-zlib \
		--disable-jpeg \
		--disable-old-jpeg \
		--disable-zstd \
		--disable-webp \
		--disable-jbig

.PHONY: configure
configure: clean-configure $(configure-tiff)

$(build-tiff): $(configure-tiff)
	$(MAKE) -C src -j 2
	$(MAKE) print-lib-deps-$(PLATFORM)

#
# INSTALLATION
#

.PHONY: install
install: $(build-tiff)
	$(MAKE) -C src install

.PHONY: print-lib-deps
print-lib-deps: print-lib-deps-$(PLATFORM)

.PHONY: print-lib-deps-Darwin
print-lib-deps-Darwin:
	@echo -e "\nExamining library dependencies..."
	cd src/ && find . \( -name "*.dylib" -o -name "*.so" \) -exec otool -L {} \;

.PHONY: print-lib-deps-Linux
print-lib-deps-Linux:
	@echo -e "\nExamining library dependencies..."
	cd src/  && find . -name "*.so" -print -exec ldd {} \;
