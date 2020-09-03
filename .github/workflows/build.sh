#!/bin/bash

set -e

# Build the modified Go compiler.
git clone --depth 1  https://github.com/jerrinsg/go-pmem ~/go-pmem
cd ~/go-pmem/src
./make.bash

cd $GOPATH/src/github.com/vmware/go-pmem-transaction/txtest

# Force travis CI to use the compiler and toolchain we just built.
# If the test is run as ~/go-pmem/bin/go test, travis is using the tools found
# in /home/travis/.gimme/versions/go1.11.1.linux.amd64 to do the build (TODO).
GOROOT="$HOME/go-pmem/" GOTOOLDIR="$HOME/go-pmem/pkg/tool/linux_amd64" ~/go-pmem/bin/go test

cd $GOPATH/src/github.com/vmware/go-pmem-transaction/txtest/crashtest
GOROOT="$HOME/go-pmem/" GOTOOLDIR="$HOME/go-pmem/pkg/tool/linux_amd64" ~/go-pmem/bin/go test -tags="crash"
GOROOT="$HOME/go-pmem/" GOTOOLDIR="$HOME/go-pmem/pkg/tool/linux_amd64" ~/go-pmem/bin/go test -tags="crash"
