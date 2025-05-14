# ckan-clamav-service

![Run tests](https://github.com/okfn/ckan-clamav-service/workflows/Run%20tests/badge.svg?branch=main)
[![codecov](https://codecov.io/gh/okfn/ckan-clamav-service/branch/main/graph/badge.svg?token=9r8Dtmd8RF)](https://codecov.io/gh/okfn/ckan-clamav-service)

A standalone web service for scanning resources uploaded to CKAN with [Clam AV](https://www.clamav.net/). Based on [CKAN Service Provider](https://github.com/ckan/ckan-service-provider)

## Local Development

* Install Clam AV
  * Alpine: `apk add clamav clamav-libunrar`
  * Ubuntu: `apt install clamav`
* Run `make help` for common tasks.
