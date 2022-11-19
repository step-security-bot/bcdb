# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## Added

- **! Added `Table.remove_row(self: Self@Table, where: Callable[[tuple[Any, ...]], bool], *, limit: int = 1000) -> int`.**
- **Added `Database.get_table(self: Self@Database, table_name: str) -> Table` which returns an existing table.**
- Added `Table.write_rows(self: Self@Table, rows: list[tuple[Any, ...]], *, i_know_what_im_doing: bool = False) -> None`. Usage is discouraged!
- Added argument `lock` to `Table.add_row`. Don't change it!

## [0.1.1] - 2022-11-18

## Added

- If the `directory` argument to `Database()` is a string, it will be converted to `pathlib.Path`.

## [0.1.0] - 2022-11-17
