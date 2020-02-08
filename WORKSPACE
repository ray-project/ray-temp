workspace(name = "com_github_ray_project_ray")

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository", "new_git_repository")
load("//java:repo.bzl", "java_repositories")

java_repositories()

git_repository(
    name = "com_github_checkstyle_java",
    commit = "85f37871ca03b9d3fee63c69c8107f167e24e77b",
    remote = "https://github.com/ruifangChen/checkstyle_java",
)

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

git_repository(
    name = "com_github_nelhage_rules_boost",
    commit = "6d6fd834281cb8f8e758dd9ad76df86304bf1869",
    remote = "https://github.com/nelhage/rules_boost",
)

load("@com_github_nelhage_rules_boost//:boost/boost.bzl", "boost_deps")

boost_deps()

git_repository(
    name = "com_github_google_flatbuffers",
    commit = "63d51afd1196336a7d1f56a988091ef05deb1c62",
    remote = "https://github.com/google/flatbuffers.git",
)

git_repository(
    name = "com_google_googletest",
    commit = "3306848f697568aacf4bcca330f6bdd5ce671899",
    remote = "https://github.com/google/googletest",
)

git_repository(
    name = "com_github_gflags_gflags",
    remote = "https://github.com/gflags/gflags.git",
    tag = "v2.2.2",
)

new_git_repository(
    name = "com_github_google_glog",
    build_file = "@//bazel:BUILD.glog",
    commit = "5c576f78c49b28d89b23fbb1fc80f54c879ec02e",
    remote = "https://github.com/google/glog",
)

new_git_repository(
    name = "plasma",
    build_file = "@//bazel:BUILD.plasma",
    commit = "d00497b38be84fd77c40cbf77f3422f2a81c44f9",
    remote = "https://github.com/apache/arrow",
)

new_git_repository(
    name = "cython",
    build_file = "@//bazel:BUILD.cython",
    commit = "49414dbc7ddc2ca2979d6dbe1e44714b10d72e7e",
    remote = "https://github.com/cython/cython",
)

load("@//bazel:python_configure.bzl", "python_configure")

python_configure(name = "local_config_python")

git_repository(
    name = "io_opencensus_cpp",
    commit = "3aa11f20dd610cb8d2f7c62e58d1e69196aadf11",
    remote = "https://github.com/census-instrumentation/opencensus-cpp",
)

# OpenCensus depends on Abseil so we have to explicitly pull it in.
# This is how diamond dependencies are prevented.
git_repository(
    name = "com_google_absl",
    commit = "aa844899c937bde5d2b24f276b59997e5b668bde",
    remote = "https://github.com/abseil/abseil-cpp",
)

# OpenCensus depends on jupp0r/prometheus-cpp
git_repository(
    name = "com_github_jupp0r_prometheus_cpp",
    commit = "60eaa4ea47b16751a8e8740b05fe70914c68a480",
    remote = "https://github.com/jupp0r/prometheus-cpp.git",
    patches = [
        # https://github.com/jupp0r/prometheus-cpp/pull/225
        "//thirdparty/patches:prometheus-windows-zlib.patch",
        "//thirdparty/patches:prometheus-windows-pollfd.patch",
    ]
)

load("@com_github_jupp0r_prometheus_cpp//bazel:repositories.bzl", "prometheus_cpp_repositories")

prometheus_cpp_repositories()
