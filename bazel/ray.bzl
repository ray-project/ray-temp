load("@com_github_google_flatbuffers//:build_defs.bzl", "flatbuffer_library_public")
load("@com_github_checkstyle_java//checkstyle:checkstyle.bzl", "checkstyle_test")
load("@bazel_common//tools/maven:pom_file.bzl", "pom_file")

COPTS = ["-DRAY_USE_GLOG", "-Wno-range-loop-analysis"] + select({
    "@bazel_tools//src/conditions:windows": [
        "-Wno-builtin-macro-redefined",  # To get rid of warnings caused by deterministic build macros (e.g. #define __DATE__ "redacted")
        "-Wno-microsoft-unqualified-friend",  # This shouldn't normally be enabled, but otherwise we get: google/protobuf/map_field.h: warning: unqualified friend declaration referring to type outside of the nearest enclosing namespace is a Microsoft extension; add a nested name specifier (for: friend class DynamicMessage)
        # TODO(mehrdadn): (How to) support dynamic linking?
        "-DRAY_STATIC",
    ],
    "//conditions:default": [
    ],
})

PYX_COPTS = [] + select({
    "@bazel_tools//src/conditions:windows": [
        "/FI" + "src/shims/windows/python-nondebug.h",
    ],
    "//conditions:default": [
    ],
})

PYX_SRCS = [] + select({
    "@bazel_tools//src/conditions:windows": [
        "src/shims/windows/python-nondebug.h",
    ],
    "//conditions:default": [
    ],
})

def flatbuffer_py_library(name, srcs, outs, out_prefix, includes = [], include_paths = []):
    flatbuffer_library_public(
        name = name,
        srcs = srcs,
        outs = outs,
        language_flag = "-p",
        out_prefix = out_prefix,
        include_paths = include_paths,
        includes = includes,
    )

def define_java_module(
        name,
        additional_srcs = [],
        exclude_srcs = [],
        additional_resources = [],
        define_test_lib = False,
        test_deps = [],
        **kwargs):
    lib_name = "io_ray_ray_" + name
    pom_file_targets = [lib_name]
    native.java_library(
        name = lib_name,
        srcs = additional_srcs + native.glob(
            [name + "/src/main/java/**/*.java"],
            exclude = exclude_srcs,
        ),
        resources = native.glob([name + "/src/main/resources/**"]) + additional_resources,
        **kwargs
    )
    checkstyle_test(
        name = "io_ray_ray_" + name + "-checkstyle",
        target = ":io_ray_ray_" + name,
        config = "//java:checkstyle.xml",
        suppressions = "//java:checkstyle-suppressions.xml",
        size = "small",
        tags = ["checkstyle"],
    )
    if define_test_lib:
        test_lib_name = "io_ray_ray_" + name + "_test"
        pom_file_targets.append(test_lib_name)
        native.java_library(
            name = test_lib_name,
            srcs = native.glob([name + "/src/test/java/**/*.java"]),
            deps = test_deps,
        )
        checkstyle_test(
            name = "io_ray_ray_" + name + "_test-checkstyle",
            target = ":io_ray_ray_" + name + "_test",
            config = "//java:checkstyle.xml",
            suppressions = "//java:checkstyle-suppressions.xml",
            size = "small",
            tags = ["checkstyle"],
        )
    pom_file(
        name = "io_ray_ray_" + name + "_pom",
        targets = pom_file_targets,
        template_file = name + "/pom_template.xml",
        substitutions = {
            "{auto_gen_header}": "<!-- This file is auto-generated by Bazel from pom_template.xml, do not modify it. -->",
        },
    )
