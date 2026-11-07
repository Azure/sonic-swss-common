# load("@rules_cc//cc:cc_test.bzl", "cc_test")

package(default_visibility = ["//visibility:public"])

cc_test(
    name = "status_code_util_test",
    srcs = ["status_code_util_test.cpp"],
    deps = [
        "//:libswsscommon",
        "@com_github_google_glog//:glog",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "saiaclschema_ut",
    srcs = ["saiaclschema_ut.cpp"],
    deps = [
        "//:libswsscommon",
        "@com_github_google_glog//:glog",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "notification_queue_ut",
    srcs = ["notification_queue_ut.cpp"],
    deps = [
        "//:libswsscommon",
        "@com_github_google_glog//:glog",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "interface_ut",
    srcs = ["interface_ut.cpp"],
    deps = [
        "//:libswsscommon",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "vrf_ut",
    srcs = ["vrf_ut.cpp"],
    deps = [
        "//:libswsscommon",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)
