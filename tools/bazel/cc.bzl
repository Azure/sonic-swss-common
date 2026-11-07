"""cc_* wrappers that make `cxxopts` work on Bazel 6.

`cxxopts` was added to the native cc rules in Bazel 7.

TODO(bazel-ready): Remove and use cc_library/cc_binary directly when we only need to support Bazel 8+.
"""

load(":bzlmod.bzl", "IS_BZLMOD")

def _fold_cxxopts(kwargs):
    cxxopts = kwargs.pop("cxxopts", [])
    if not cxxopts:
        return kwargs
    if IS_BZLMOD:
        kwargs["cxxopts"] = cxxopts
    else:
        kwargs["copts"] = kwargs.get("copts", []) + cxxopts
    return kwargs

def swss_cc_library(**kwargs):
    native.cc_library(**_fold_cxxopts(kwargs))

def swss_cc_binary(**kwargs):
    native.cc_binary(**_fold_cxxopts(kwargs))
