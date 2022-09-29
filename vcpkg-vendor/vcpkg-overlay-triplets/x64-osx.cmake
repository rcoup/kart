set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE dynamic)
set(VCPKG_BUILD_TYPE release)

set(VCPKG_CMAKE_SYSTEM_NAME Darwin)
set(VCPKG_OSX_ARCHITECTURES x86_64)
set(VCPKG_OSX_DEPLOYMENT_TARGET "10.15")

# https://github.com/microsoft/vcpkg/issues/10038
set(VCPKG_C_FLAGS "-mmacosx-version-min=10.15")
set(VCPKG_CXX_FLAGS "-mmacosx-version-min=10.15")
set(ENV{MACOSX_DEPLOYMENT_TARGET} "10.15")
