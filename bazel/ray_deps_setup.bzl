load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository", "new_git_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def github_repository(*, name=None, remote=None, commit=None, tag=None,
                      branch=None, build_file=None, build_file_content=None,
                      sha256=None, archive_suffix=".zip", shallow_since=None,
                      strip_prefix=True, url=None, path=None, **kwargs):
    """
    Conveniently chooses between archive, git, etc. GitHub repositories.
    Prefer archives, as they're smaller and faster due to the lack of history.

    One of {commit, tag, branch} must also be provided (as usual).

    sha256 should be omitted (or None) when the archive hash is unknown, then
    updated ASAP to allow caching & avoid repeated downloads on every build.

    If remote       == None , it is an error.
    If name         == None , it is auto-deduced, but this is NOT recommended.
    If build_file   == True , it is auto-deduced.
    If strip_prefix == True , it is auto-deduced.
    If url          == None , it is auto-deduced.
    If sha256       != False, uses archive download (recommended; fast).
    If sha256       == False, uses git clone (NOT recommended; slow).
    If path         != None , local repository is assumed at the given path.
    """
    GIT_SUFFIX = ".git"

    treeish = commit or tag or branch
    if path == None and not treeish: fail("Missing commit, tag, or branch argument")
    if path == None and remote == None: fail("Missing remote argument")

    if path == None and remote.endswith(GIT_SUFFIX):
        remote_no_suffix = remote[:len(remote) - len(GIT_SUFFIX)]
    else:
        remote_no_suffix = remote
    project = remote_no_suffix.split("//", 1)[1].split("/")[2] if remote_no_suffix else None

    if project != None and name == None:
        name = project.replace("-", "_")
    if project != None and strip_prefix == True:
        strip_prefix = "%s-%s" % (project, treeish)
    if url == None:
        url = "%s/archive/%s%s" % (remote_no_suffix, treeish, archive_suffix)
    if name != None and build_file == True:
        build_file = "@//%s:%s" % ("bazel", "BUILD." + name)

    if path != None:
        if build_file or build_file_content:
            native.new_local_repository(name=name, path=path,
                                        build_file=build_file,
                                        build_file_content=build_file_content,
                                        **kwargs)
        else:
            native.local_repository(name=name, path=path, **kwargs)
    elif sha256 == False:
        if build_file or build_file_content:
            new_git_repository(name=name, remote=remote, build_file=build_file,
                               commit=commit, tag=tag, branch=branch,
                               shallow_since=shallow_since,
                               build_file_content=build_file_content,
                               strip_prefix=strip_prefix, **kwargs)
        else:
            git_repository(name=name, remote=remote, strip_prefix=strip_prefix,
                           commit=commit, tag=tag, branch=branch,
                           shallow_since=shallow_since, **kwargs)
    else:
        http_archive(name=name, url=url, sha256=sha256, build_file=build_file,
                     strip_prefix=strip_prefix,
                     build_file_content=build_file_content, **kwargs)

def ray_deps_setup():
    github_repository(
        name = "redis",
        build_file = True,
        tag = "5.0.3",
        remote = "https://github.com/antirez/redis",
        sha256 = "8e5997b447b1afdd1efd33731968484d2fe71c271fa7f1cd6b2476367e964e0e",
        patches = [
            "//thirdparty/patches:hiredis-async-include-dict.patch",
            "//thirdparty/patches:hiredis-casts.patch",
            "//thirdparty/patches:hiredis-connect-rename.patch",
            "//thirdparty/patches:hiredis-windows-sigpipe.patch",
            "//thirdparty/patches:hiredis-windows-sockets.patch",
            "//thirdparty/patches:hiredis-windows-strerror.patch",
            "//thirdparty/patches:hiredis-windows-poll.patch",
            "//thirdparty/patches:redis-windows-poll.patch",
        ],
    )

    github_repository(
        name = "rules_jvm_external",
        tag = "2.10",
        remote = "https://github.com/bazelbuild/rules_jvm_external",
        sha256 = "1bbf2e48d07686707dd85357e9a94da775e1dbd7c464272b3664283c9c716d26",
    )

    github_repository(
        name = "bazel_common",
        commit = "f1115e0f777f08c3cdb115526c4e663005bec69b",
        remote = "https://github.com/google/bazel-common",
        sha256 = "1e05a4791cc3470d3ecf7edb556f796b1d340359f1c4d293f175d4d0946cf84c",
    )

    github_repository(
        name = "com_github_checkstyle_java",
        commit = "ef367030d1433877a3360bbfceca18a5d0791bdd",
        remote = "https://github.com/ray-project/checkstyle_java",
        sha256 = "2fc33ec804011a03106e76ae77d7f1b09091b0f830f8e2a0408f079a032ed716",
    )

    http_archive(
        # This rule is used by @com_github_nelhage_rules_boost and
        # declaring it here allows us to avoid patching the latter.
        name = "boost",
        build_file = "@com_github_nelhage_rules_boost//:BUILD.boost",
        sha256 = "96b34f7468f26a141f6020efb813f1a2f3dfb9797ecf76a7d7cbd843cc95f5bd",
        strip_prefix = "boost_1_71_0",
        url = "https://dl.bintray.com/boostorg/release/1.71.0/source/boost_1_71_0.tar.gz",
        patches = [
            "//thirdparty/patches:boost-exception-no_warn_typeid_evaluated.patch",
        ],
    )

    github_repository(
        name = "com_github_nelhage_rules_boost",
        # If you update the Boost version, remember to update the 'boost' rule.
        commit = "9eff8676ce431232ae625ee93c4123f7c03477eb",
        remote = "https://github.com/nelhage/rules_boost",
        sha256 = "f68166c117398e1c624af8dd14115dbbc3761137599e806007271abbb8cd002b",
        patches = [
            "//thirdparty/patches:rules_boost-undefine-boost_fallthrough.patch",
            "//thirdparty/patches:rules_boost-windows-linkopts.patch",
        ],
    )

    github_repository(
        name = "com_github_google_flatbuffers",
        commit = "63d51afd1196336a7d1f56a988091ef05deb1c62",
        remote = "https://github.com/google/flatbuffers",
        sha256 = "dd87be0acf932c9b0d9b5d7bb49aec23e1c98bbd3327254bd90cb4af198f9332",
    )

    github_repository(
        name = "com_google_googletest",
        commit = "3306848f697568aacf4bcca330f6bdd5ce671899",
        remote = "https://github.com/google/googletest",
        sha256 = "2625a1d301cd658514e297002170c2fc83a87beb0f495f943601df17d966511d",
    )

    github_repository(
        name = "com_github_gflags_gflags",
        commit = "e171aa2d15ed9eb17054558e0b3a6a413bb01067",
        remote = "https://github.com/gflags/gflags",
        sha256 = "da72f0dce8e3422d0ab2fea8d03a63a64227b0376b3558fd9762e88de73b780b",
    )

    github_repository(
        name = "com_github_google_glog",
        commit = "925858d9969d8ee22aabc3635af00a37891f4e25",
        remote = "https://github.com/google/glog",
        sha256 = "dbe787f2a7cf1146f748a191c99ae85d6b931dd3ebdcc76aa7ccae3699149c67",
        patches = [
            "//thirdparty/patches:glog-stack-trace.patch",
        ],
    )

    github_repository(
        name = "plasma",
        build_file = True,
        commit = "86f34aa07e611787d9cc98c6a33b0a0a536dce57",
        remote = "https://github.com/apache/arrow",
        sha256 = "4f1956e74188fa15078c8ad560bbc298624320d2aafd21fe7a2511afee7ea841",
        patches = [
            "//thirdparty/patches:arrow-headers-unused.patch",
            "//thirdparty/patches:arrow-windows-export.patch",
            "//thirdparty/patches:arrow-windows-poll.patch",
            "//thirdparty/patches:arrow-windows-sigpipe.patch",
            "//thirdparty/patches:arrow-windows-socket.patch",
        ],
    )

    github_repository(
        name = "cython",
        build_file = True,
        commit = "49414dbc7ddc2ca2979d6dbe1e44714b10d72e7e",
        remote = "https://github.com/cython/cython",
        sha256 = "aaee5dec23165ee10c189d8b40f19861e2c6929c015cee3d2b4e56d8a1bdc422",
    )

    github_repository(
        name = "io_opencensus_cpp",
        commit = "3aa11f20dd610cb8d2f7c62e58d1e69196aadf11",
        remote = "https://github.com/census-instrumentation/opencensus-cpp",
        sha256 = "92eef77c44d01e8472f68a2f1329919a1bb59317a4bb1e4d76081ab5c13a56d6",
    )

    # OpenCensus depends on Abseil so we have to explicitly pull it in.
    # This is how diamond dependencies are prevented.
    github_repository(
        name = "com_google_absl",
        commit = "aa844899c937bde5d2b24f276b59997e5b668bde",
        remote = "https://github.com/abseil/abseil-cpp",
        sha256 = "f1a959a2144f0482b9bd61e67a9897df02234fff6edf82294579a4276f2f4b97",
    )

    # OpenCensus depends on jupp0r/prometheus-cpp
    github_repository(
        name = "com_github_jupp0r_prometheus_cpp",
        commit = "60eaa4ea47b16751a8e8740b05fe70914c68a480",
        remote = "https://github.com/jupp0r/prometheus-cpp",
        sha256 = "9756bd2d573e7722f97dbe6d35934e43b9a79e6a87fc5e1da79774a621cddd8e",
        patches = [
            # https://github.com/jupp0r/prometheus-cpp/pull/225
            "//thirdparty/patches:prometheus-windows-zlib.patch",
            "//thirdparty/patches:prometheus-windows-pollfd.patch",
        ]
    )

    github_repository(
        name = "com_github_grpc_grpc",
        # NOTE: If you update this, also update @boringssl's hash.
        commit = "4790ab6d97e634a1ede983be393f3bb3c132b2f7",
        remote = "https://github.com/grpc/grpc",
        sha256 = "723853c36ea6d179d32a4f9f2f8691dbe0e28d5bbc521c954b34355a1c952ba5",
        patches = [
            "//thirdparty/patches:grpc-command-quoting.patch",
            "//thirdparty/patches:grpc-cython-copts.patch",
        ],
    )

    github_repository(
        # This rule is used by @com_github_grpc_grpc, and using a GitHub mirror
        # provides a deterministic archive hash for caching. Explanation here:
        # https://github.com/grpc/grpc/blob/4790ab6d97e634a1ede983be393f3bb3c132b2f7/bazel/grpc_deps.bzl#L102
        name = "boringssl",
        # Ensure this matches the commit used by grpc's bazel/grpc_deps.bzl
        commit = "83da28a68f32023fd3b95a8ae94991a07b1f6c62",
        remote = "https://github.com/google/boringssl",
        sha256 = "58bdaf1fa305d42142c0c1aa7a84aa2e5df12f581c13a606b20242e1d037210c",
    )

    github_repository(
        name = "rules_proto_grpc",
        commit = "a74fef39c5fe636580083545f76d1eab74f6450d",
        remote = "https://github.com/rules-proto-grpc/rules_proto_grpc",
        sha256 = "53561ecacaebe58916dfdb962d889a56394d3fae6956e0bcd63c4353f813284a",
    )
