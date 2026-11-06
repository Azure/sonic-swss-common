*static analysis:*

[![Total alerts](https://img.shields.io/lgtm/alerts/g/sonic-net/sonic-swss-common.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/sonic-net/sonic-swss-common/alerts/)
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/sonic-net/sonic-swss-common.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/sonic-net/sonic-swss-common/context:python)
[![Language grade: C/C++](https://img.shields.io/lgtm/grade/cpp/g/sonic-net/sonic-swss-common.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/sonic-net/sonic-swss-common/context:cpp)

*sonic-swss-common builds:*

[![master build](https://dev.azure.com/mssonic/build/_apis/build/status/Azure.sonic-swss-common?branchName=master&label=master)](https://dev.azure.com/mssonic/build/_build/latest?definitionId=9&branchName=master)
[![202205 build](https://dev.azure.com/mssonic/build/_apis/build/status/Azure.sonic-swss-common?branchName=202205&label=202205)](https://dev.azure.com/mssonic/build/_build/latest?definitionId=9&branchName=202205)
[![202111 build](https://dev.azure.com/mssonic/build/_apis/build/status/Azure.sonic-swss-common?branchName=202111&label=202111)](https://dev.azure.com/mssonic/build/_build/latest?definitionId=9&branchName=202111)
[![202106 build](https://dev.azure.com/mssonic/build/_apis/build/status/Azure.sonic-swss-common?branchName=202106&label=202106)](https://dev.azure.com/mssonic/build/_build/latest?definitionId=9&branchName=202106)
[![202012 build](https://dev.azure.com/mssonic/build/_apis/build/status/Azure.sonic-swss-common?branchName=202012&label=202012)](https://dev.azure.com/mssonic/build/_build/latest?definitionId=9&branchName=202012)
[![201911 build](https://dev.azure.com/mssonic/build/_apis/build/status/Azure.sonic-swss-common?branchName=201911&label=201911)](https://dev.azure.com/mssonic/build/_build/latest?definitionId=9&branchName=201911)

# SONiC - SWitch State Service Common Library - SWSS-COMMON

## Description
The SWitch State Service (SWSS) common library provides libraries for database communications, netlink wrappers, and other functions needed by SWSS.

## Getting Started

### Build from Source

Checkout the source:

    git clone --recursive https://github.com/sonic-net/sonic-swss-common


Install build dependencies:

    sudo apt-get install make libtool m4 autoconf dh-exec debhelper cmake pkg-config \
                         libhiredis-dev libnl-3-dev libnl-genl-3-dev libnl-route-3-dev \
                         libnl-nf-3-dev swig3.0 libpython2.7-dev libpython3-dev \
                         libgtest-dev libgmock-dev libboost-dev

Build and Install Google Test and Mock from DEB source packages:

    cd /usr/src/gtest && sudo cmake . && sudo make

You can compile and install from source using:

    ./autogen.sh
    ./configure
    make && sudo make install

You can also build a debian package using:

    ./autogen.sh
    ./configure
    dpkg-buildpackage -us -uc -b

### Build with Google Test
1. Rebuild with Google Test
```
$ ./autogen.sh
$ ./configure --enable-debug 'CXXFLAGS=-O0 -g'
$ make clean
$ GCC_COLORS=1 make
```

2. Start redis server if not yet:
```
sudo sed -i 's/notify-keyspace-events ""/notify-keyspace-events AKE/' /etc/redis/redis.conf
sudo service redis-server start
```

3. Run unit test:
```
tests/tests
```

## Need Help?

For general questions, setup help, or troubleshooting:
- [sonicproject on Google Groups](https://groups.google.com/g/sonicproject)

For bug reports or feature requests, please open an Issue.

## Contribution guide

Please read the [contributors guide](https://github.com/sonic-net/SONiC/blob/gh-pages/CONTRIBUTING.md) for information about how to contribute.

All contributors must sign an [Individual Contributor License Agreement (ICLA)](https://docs.linuxfoundation.org/lfx/easycla/v2-current/contributors/individual-contributor) before contributions can be accepted. This process is managed by the [Linux Foundation - EasyCLA](https://easycla.lfx.linuxfoundation.org/) and automated
via a GitHub bot. If the contributor has not yet signed a CLA, the bot will create a comment on the pull request containing a link to electronically sign the CLA.

### GitHub Workflow

We're following basic GitHub Flow. If you have no idea what we're talking about, check out [GitHub's official guide](https://guides.github.com/introduction/flow/). Note that merge is only performed by the repository maintainer.

Guide for performing commits:

* Isolate each commit to one component/bugfix/issue/feature
* Use a standard commit message format:

>     [component/folder touched]: Description intent of your changes
>
>     [List of changes]
>
> 	  Signed-off-by: Your Name your@email.com

For example:

>     swss-common: Stabilize the ConsumerTable
>
>     * Fixing autoreconf
>     * Fixing unit-tests by adding checkers and initialize the DB before start
>     * Adding the ability to select from multiple channels
>     * Health-Monitor - The idea of the patch is that if something went wrong with the notification channel,
>       we will have the option to know about it (Query the LLEN table length).
>
>       Signed-off-by: user@dev.null


* Each developer should fork this repository and [add the team as a Contributor](https://help.github.com/articles/adding-collaborators-to-a-personal-repository)
* Push your changes to your private fork and do "pull-request" to this repository
* Use a pull request to do code review
* Use issues to keep track of what is going on

## Explicit DBConnector authentication

Existing endpoint constructors keep their transport selection and do not consult writer
policy. To request native local authentication, pass the namespace (or
`SonicDBKey`) followed by `authenticate=true`:

```cpp
swss::DBConnector db("STATE_DB", 1000, false, std::string(), true);
```

The existing Python binding exposes the same overload:

```python
db = swsscommon.DBConnector("STATE_DB", 1000, False, "", True)
```

Authentication runs before database selection and is repeated for copied and
secondary connections. It supports explicitly selected Unix sockets. Startup
provides the root-owned `/run/redis-auth/local.json` endpoint policy and
`local-system/writer.secret`. Missing policy or an unmatched endpoint leaves the
default account in use. A non-root caller unable to read the secret remains a
reader; invalid policy or rejected credentials fail construction. Opt-in does
not grant access to credentials. Nonzero constructor timeouts also bound AUTH.

This addition changes C++ object layout; rebuild dependent native applications
and language bindings with the matching library. Higher-level connector
constructor options and raw-client adapters are separate follow-ups.

The focused tests require an explicitly selected disposable root container:
`SWSS_AUTH_TEST_CONTAINER=1 pytest --noconftest tests/test_redis_authentication.py`.
They refuse to replace an existing authentication-policy directory.

