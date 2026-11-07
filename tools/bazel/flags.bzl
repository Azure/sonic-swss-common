"""Compiler and linker flags shared across the sonic-swss-common build."""

load(":bzlmod.bzl", "IS_BZLMOD")

# CXXFLAGS that we need for Bazel specifically. Not present in the Makefile
CXXFLAGS_COMMON_BAZEL = [
    # TODO(bazel-ready): rules_distroless introduces a bunch of include directories that don't exist
    # so we need to disable that warning or else -Werror will fail the build.
    "-Wno-missing-include-dirs",
]

# CFLAGS_COMMON from configure.ac, which is used both for C and C++
CXXFLAGS_COMMON_MAKEFILE = [
    "-ansi",
    "-fPIC",
    "-std=c++14",
    "-Wall",
    "-Wcast-align",
    "-Wcast-qual",
    "-Wconversion",
    "-Wdisabled-optimization",
    "-Werror",
    "-Wextra",
    "-Wfloat-equal",
    "-Wformat=2",
    "-Wformat-nonliteral",
    "-Wformat-security",
    "-Wformat-y2k",
    "-Wimport",
    "-Winit-self",
    "-Winvalid-pch",
    "-Wlong-long",
    "-Wmissing-field-initializers",
    "-Wmissing-format-attribute",
    "-Wmissing-include-dirs",
    "-Wmissing-noreturn",
    "-Wno-aggregate-return",
    "-Wno-padded",
    "-Wno-switch-enum",
    "-Wno-unused-parameter",
    "-Wpacked",
    "-Wpointer-arith",
    "-Wredundant-decls",
    "-Wshadow",
    "-Wstack-protector",
    "-Wstrict-aliasing=3",
    "-Wswitch",
    "-Wswitch-default",
    "-Wunreachable-code",
    "-Wunused",
    "-Wvariadic-macros",
    "-Wno-write-strings",
    "-Wno-missing-format-attribute",
    "-Wno-long-long",
    "-fstack-protector-strong",
]

CXXFLAGS_COMMON = CXXFLAGS_COMMON_MAKEFILE + CXXFLAGS_COMMON_BAZEL

DBGFLAGS = select({
    "@sonic_build_infra//:debug_enabled": [
        "-ggdb",
        "-gdwarf-5",
    ],
    "//conditions:default": ["-g"],
    # TODO(bazel-ready): Remove when we only have to support Bazel 8+.
}) if IS_BZLMOD else ["-g"]
