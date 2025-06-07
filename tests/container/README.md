# Docker base image for testing simple-sqlite3-orm on supported Ubuntu version

## Test base image build

```shell
BASE_URI=ghcr.io/pga2rn/pga2rn/simple-sqlite3-orm/test_base
docker build \
    --build-arg=UBUNTU_BASE=ubuntu:18.04 \
    --output type=image,name=${BASE_URI}:ubuntu_18.04,compression=zstd,compression-level=19,oci-mediatypes=true,force-compression=true \
    .
```
