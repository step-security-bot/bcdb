# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## Added

- **! Added `Table.get_row_where(self: Self@Table, attribute_name: str, attribute_value: Any) -> tuple[Any, ...]`.**

## [0.3.0] - 2022-11-25

## Added

- **! Added `Table.contains(self: Self@Table, attribute_name: str, attribute_value: Any) -> bool`.**
- **! Added `Table.not_contains(self: Self@Table, attribute_name: str, attribute_value: Any) -> bool`.**
- **! Added `Table.map(self: Self@Table, func: (tuple[Any, ...]) -> (tuple[Any, ...] | None), *, write: bool = False) -> list[tuple[Any, ...]]`.**
- **! Added `Table.filter(self: Self@Table, func: (tuple[Any, ...]) -> bool, *, write: bool = False) -> list[tuple[Any, ...]]`.**
- **! Added `Table.contains_row(self: Self@Table, row: tuple[Any, ...]) -> bool`.**
- **! Added `Table.not_contains_row(self: Self@Table, row: tuple[Any, ...]) -> bool`.**
- Added `Table.add_rows(self: Self@Table, rows: Iterable[tuple[Any, ...]], *, lock: bool = True) -> None`.
- Added `Table.get_attribute_index(self: Self@Table, name: str) -> int`.

## Fixed

- **Fixed problem with writing `\n` and `\r` to database.**
- `Database.add_table` checks that the table's name is unique, and its attributes' names are unique.

## [0.2.0] - 2022-11-19

## Added

- **! Added `Table.remove_row(self: Self@Table, where: Where, must_remove: bool = True) -> bool`.**
- **! Added `Table.remove_rows(self: Self@Table, where: Where, *, limit: int = 1000) -> int`.**
- **Added `Database.get_table(self: Self@Database, table_name: str) -> Table` which returns an existing table.**
- Added `Table.write_rows(self: Self@Table, rows: list[tuple[Any, ...]], *, i_know_what_im_doing: bool = False) -> None`. Usage is discouraged!
- Added argument `lock` to `Table.add_row`. Don't change it!

## [0.1.1] - 2022-11-18

## Added

- If the `directory` argument to `Database()` is a string, it will be converted to `pathlib.Path`.

## [0.1.0] - 2022-11-17
