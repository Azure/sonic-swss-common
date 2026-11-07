"""Load-time detection of whether Bazel is running with bzlmod enabled.

This repository needs to build in both Bazel 8 (for sonic-buidimage)
and Bazel 6 (for p4rt).

This constant allows us to work around bzlmod-only repositories like @rules_distroless.

TODO(bazel-ready): Remove when we only need to support Bazel 8+.
"""

IS_BZLMOD = str(Label("//:invalid")).startswith("@@")
