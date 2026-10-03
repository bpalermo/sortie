load("@envoy//bazel:envoy_build_system.bzl", "envoy_package")
load("@rules_python//python:defs.bzl", "py_binary", "py_library", "py_test")

licenses(["notice"])  # Apache 2

envoy_package()

py_binary(
    name = "benchmarks",
    srcs = ["static_benchmarks.py"],
    main = ":static_benchmarks.py",
    deps = [
        ":benchmarks_envoy_proxy_lib",
        ":utilities_lib",
        "//engine/test/integration:integration_test_base_lean",
    ],
)

genrule(
    name = "static_benchmarks_test",
    srcs = ["static_benchmarks.py"],
    outs = ["static_benchmarks_test.py"],
    cmd = """
    cat $(location :static_benchmarks.py) > $@
    """,
)

py_test(
    name = "test_benchmarks",
    size = "large",
    # Needs the gperftools cpu profiler, so only passes under the opt build
    # upstream's benchmark CI task uses (-c opt --define tcmalloc=gperftools);
    # excluded from //... until that configuration exists here.
    tags = ["manual"],
    srcs = [
        "static_benchmarks_test.py",
        "test/test_discovery.py",
    ],
    exec_properties = {
    },
    main = "static_benchmarks_test.py",
    deps = [
        ":benchmarks_envoy_proxy_lib",
        ":utilities_lib",
        "//engine/test/integration:integration_test_base",
    ],
)

py_library(
    name = "benchmarks_envoy_proxy_lib",
    srcs = [
        "envoy_proxy.py",
    ],
    data = [
        "configurations/envoy_proxy.yaml",
    ],
    srcs_version = "PY2AND3",
    deps = [
        "@rules_python//python/runfiles",
    ],
)

py_library(
    name = "benchmarks_dynamic_envoy_proxy_lib",
    srcs = [
        "dynamic_config_envoy_proxy.py",
        "envoy_proxy.py",
    ],
    data = [
        "configurations/dynamic_resources.yaml",
        "configurations/dynamic_resources_eds.yaml",
        "configurations/lds.yaml",
        "configurations/request_source_five_clusters.json",
    ],
    srcs_version = "PY2AND3",
    deps = [
        "//engine/api/configuration:cluster_config_manager_py_proto",
        "@rules_python//python/runfiles",
    ],
)

genrule(
    name = "dynamic_benchmarks_test",
    srcs = ["dynamic_benchmarks.py"],
    outs = ["dynamic_benchmarks_test.py"],
    cmd = """
    cat $(location :dynamic_benchmarks.py) > $@
    """,
)

py_test(
    name = "test_dynamic_benchmarks",
    size = "large",
    # Needs the gperftools cpu profiler, so only passes under the opt build
    # upstream's benchmark CI task uses (-c opt --define tcmalloc=gperftools);
    # excluded from //... until that configuration exists here.
    tags = ["manual"],
    srcs = [
        "dynamic_benchmarks_test.py",
        "dynamic_test/test_cds_churn_with_traffic.py",
        "dynamic_test/test_eds_churn_with_traffic.py",
    ],
    exec_properties = {
    },
    main = "dynamic_benchmarks_test.py",
    srcs_version = "PY2AND3",
    deps = [
        ":benchmarks_dynamic_envoy_proxy_lib",
        ":utilities_lib",
        "//engine/dynamic_config:dynamic_config_server",
        "//engine/test/integration:integration_test_base",
    ],
)

py_binary(
    name = "dynamic_benchmarks",
    srcs = [
        "dynamic_benchmarks.py",
    ],
    main = "dynamic_benchmarks.py",
    srcs_version = "PY2AND3",
    deps = [
        ":benchmarks_dynamic_envoy_proxy_lib",
        ":utilities_lib",
        "//engine/dynamic_config:dynamic_config_server",
        "//engine/test/integration:integration_test_base_lean",
    ],
)

py_library(
    name = "utilities_lib",
    srcs = [
        "utilities.py",
    ],
    data = [
        "templates/simple_plot.html",
    ],
    srcs_version = "PY2AND3",
    deps = [
        "@rules_python//python/runfiles",
    ],
)
