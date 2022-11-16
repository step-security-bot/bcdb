# Black Cat DataBase

[![Hits-of-Code](https://hitsofcode.com/github/koviubi56/bcdb?branch=main)](https://hitsofcode.com/github/koviubi56/bcdb/view?branch=main)
<!-- ![Codacy grade](https://img.shields.io/codacy/grade/42424fcd258a44f3a0303ca6ca535f67) -->
![CodeFactor Grade](https://img.shields.io/codefactor/grade/github/koviubi56/bcdb)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/koviubi56/bcdb/main.svg)](https://results.pre-commit.ci/latest/github/koviubi56/bcdb/main)
![CircleCI](https://img.shields.io/circleci/build/github/koviubi56/bcdb)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
![semantic-release](https://img.shields.io/badge/%F0%9F%93%A6%F0%9F%9A%80-semantic--release-e10079.svg)
![GitHub](https://img.shields.io/github/license/koviubi56/bcdb)
![PyPI](https://img.shields.io/pypi/v/bcdb)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/bcdb)
![PyPI - Format](https://img.shields.io/pypi/format/bcdb)

Black Cat DataBase (BCDB) is a simple database.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install bcdb. _[Need more help?](https://packaging.python.org/en/latest/tutorials/installing-packages/)_

```bash
pip install bcdb
```

## Requirements

BCDB requires Python 3.10.

## Usage

```python
import bcdb
from pathlib import Path

db = bcdb.Database(Path("database_directory"))
table = db.add_table("tablename", [
    bcdb.Attribute(
        "attribute1",
        bcdb.AttributeType.STRING,
        bcdb.AttributeRequirements.UNIQUE,
    ),
    bcdb.Attribute(
        "attribute2",
        bcdb.AttributeType.FLOAT,
    ),
])
table.add_row(("hello", 3.14))
table.add_row(("world", -0.5))
```

For more information see the docstrings.

## Support

Questions should be asked in the [Discussions tab](https://github.com/koviubi56/bcdb/discussions/categories/q-a).

Feature requests and bug reports should be reported in the [Issues tab](https://github.com/koviubi56/bcdb/issues/new/choose).

Security vulnerabilities should be reported as described in our [Security policy](https://github.com/koviubi56/bcdb/security/policy) (the [SECURITY.md](SECURITY.md) file).

## Contributing

[Pull requests](https://github.com/koviubi56/bcdb/blob/main/CONTRIBUTING.md#pull-requests) are welcome. For major changes, please [open an issue first](https://github.com/koviubi56/bcdb/issues/new/choose) to discuss what you would like to change.

Please make sure to add entries to [the changelog](CHANGELOG.md).

For more information, please read the [contributing guidelines](CONTRIBUTING.md).

## Authors and acknowledgments

A list of nice people who helped this project can be found in the [CONTRIBUTORS file](CONTRIBUTORS).

## License

[GNU GPLv3+](LICENSE)
