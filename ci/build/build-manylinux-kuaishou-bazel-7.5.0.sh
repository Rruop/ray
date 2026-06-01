#!/bin/bash
# Build an environment for building ray wheels for the manylinux2014 platform.

set -exuo pipefail

BAZELISK_VERSION="v1.26.0"

ARCH="$(uname -m)"

case "$ARCH" in
  x86_64|amd64)
    ARCH="x86_64"
    ;;
  aarch64|arm64)
    ARCH="aarch64"
    ;;
  *)
    echo "Unsupported arch: $ARCH" >&2
    exit 1
    ;;
esac

echo "Architecture is ${ARCH}"

if [[ ! -e /usr/bin/nproc ]]; then
  echo -e '#!/bin/bash\necho 10' > "/usr/bin/nproc"
  chmod +x /usr/bin/nproc
fi

# Install ray cpp dependencies.
sudo yum -y install unzip zip sudo openssl xz less vim wget
if [[ "${ARCH}" == "x86_64" ]]; then
  sudo yum -y install libasan-4.8.5-44.el7.x86_64 libubsan-7.3.1-5.10.el7.x86_64 \
    devtoolset-8-libasan-devel.x86_64
fi

# Install ray java dependencies.
if [[ "${RAYCI_DISABLE_JAVA:-false}" != "true" && "${RAY_INSTALL_JAVA:-1}" == "1" ]]; then
  sudo yum -y install java-1.8.0-openjdk java-1.8.0-openjdk-devel maven
  java -version
  JAVA_BIN="$(readlink -f "$(command -v java)")"
  echo "java_bin path ${JAVA_BIN}"
  export JAVA_HOME="${JAVA_BIN%jre/bin/java}"
fi

# Install nodejs
NODE_VERSION_FULL="14.21.3"

if [[ "${ARCH}" == "x86_64" ]]; then
  NODE_URL="https://nodejs.org/dist/v${NODE_VERSION_FULL}/node-v${NODE_VERSION_FULL}-linux-x64.tar.xz"
  NODE_SHA256="05c08a107c50572ab39ce9e8663a2a2d696b5d262d5bd6f98d84b997ce932d9a"
else # aarch64
  NODE_URL="https://nodejs.org/dist/v${NODE_VERSION_FULL}/node-v${NODE_VERSION_FULL}-linux-arm64.tar.xz"
  NODE_SHA256="f06642bfcf0b8cc50231624629bec58b183954641b638e38ed6f94cd39e8a6ef"
fi

NODE_DIR="/usr/local/node"
curl -fsSL "${NODE_URL}" -o /tmp/node.tar.xz
echo "$NODE_SHA256  /tmp/node.tar.xz" | sha256sum -c -
sudo mkdir -p "$NODE_DIR"
sudo tar -xf /tmp/node.tar.xz -C "$NODE_DIR" --strip-components=1
rm /tmp/node.tar.xz

# Install bazel
BAZEL_INSTALLER="/tmp/bazel-7.5.0-installer-linux-x86_64.sh"

chmod +x ${BAZEL_INSTALLER}
sudo ${BAZEL_INSTALLER}
rm ${BAZEL_INSTALLER}

# Install CSC tool
echo "Downloading CSC tool..."
wget -q -O /tmp/csc_linux_amd64.tar.gz https://halo.corp.kuaishou.com/api/cloud-storage/v1/public-objects/devcloud-product/cloud-storage%2Fcsc_linux_amd64_1.0.tar.gz
tar -xzf /tmp/csc_linux_amd64.tar.gz -C /tmp
chmod +x /tmp/csc
sudo mv /tmp/csc /usr/local/bin/csc
rm /tmp/csc_linux_amd64.tar.gz

# Use python3.10 as default python3
sudo ln -sf /usr/local/bin/python3.10 /usr/local/bin/python3

{
  echo "build --config=ci"
  echo "build --announce_rc"
  if [[ "${BUILDKITE_BAZEL_CACHE_URL:-}" != "" ]]; then
    echo "build:ci --remote_cache=${BUILDKITE_BAZEL_CACHE_URL:-}"
  fi
} > "$HOME"/.bazelrc
