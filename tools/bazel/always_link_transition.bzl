"""Transition to force `//:alwayslink` on for a single `cc_binary`."""

load("@bazel_skylib//lib:paths.bzl", "paths")

def _alwayslink_transition_impl(_settings, _attr):
    return {"//:alwayslink": True}

alwayslink_transition = transition(
    implementation = _alwayslink_transition_impl,
    inputs = [],
    outputs = ["//:alwayslink"],
)

# This implementation is shamelessly stolen from bazel-lib
# https://github.com/bazel-contrib/bazel-lib/blob/main/lib/transitions.bzl
def _transitioned_cc_binary_impl(ctx):
    result = []

    binary = ctx.attr.binary[0]
    default_info = binary[DefaultInfo]
    files = default_info.files.to_list()
    if len(files) != 1:
        fail("Please make sure that target {} produces exactly one file".format(ctx.label))

    original_file = files[0]

    new_file_name = original_file.basename
    new_file = ctx.actions.declare_file(paths.join(ctx.label.name, new_file_name))

    ctx.actions.run_shell(
        inputs = [original_file],
        outputs = [new_file],
        command = "cp {} {}".format(original_file.path, new_file.path),
    )
    files = depset(direct = [new_file])

    result.append(
        DefaultInfo(
            files = files,
        ),
    )

    return result

alwayslink_cc_binary = rule(
    implementation = _transitioned_cc_binary_impl,
    doc = """
Temporary rule to transition a binary with the `alwayslink_transition`.
When building `sonic-swss`, we want _most_ of the build to be built with `--@sonic_swss_common//:alwayslink=False`,
because otherwise we end up with symbols from libcommon all over the codebase.

However, we want the actual shared library of libswsscommon_consolidated.so to be built with alwayslink=True,
so we must transition that particular target to change the setting.

This is a temporary rule, which should go away as soon as we have a `cc_shared_library` implementation of libswsscommon.
    """,
    attrs = {
        "binary": attr.label(
            cfg = alwayslink_transition,
        ),
        # TODO(bazel-ready): Remove this attribute when we can stop supporting Bazel 6.
        #   Ref: https://github.com/bazelbuild/bazel/commit/bb7fb2d32f055f2a70a5ab394cf5aef29bc74b2e
        "_allowlist_function_transition": attr.label(
            default = "@bazel_tools//tools/allowlists/function_transition_allowlist",
        ),
    },
)
