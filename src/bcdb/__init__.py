"""
This file is part of Black Cat DataBase.

Copyright (C) 2022  Koviubi56

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""
__version__ = "0.1.1"
__author__ = "Koviubi56"
__email__ = "koviubi56@duck.com"
__license__ = "GNU GPLv3"
__copyright__ = "Copyright (C) 2022 Koviubi56"
__description__ = "Black Cat DataBase is a simple database."
__url__ = "https://github.com/koviubi56/bcdb"

import dataclasses
import enum
import functools
import pathlib
import string as stringlib
import threading
from typing import Any

from typing_extensions import Self

if not __debug__:
    raise Exception("BCDB cannot be used with the -O flag")


@dataclasses.dataclass(order=True, frozen=True)
class Table:
    """
    A table.

    Args:
        file (pathlib.Path): The table file.
    """

    file: pathlib.Path
    lock: Any = dataclasses.field(
        default_factory=threading.Lock, compare=False
    )

    def __post_init__(self) -> None:
        if not self.file.exists():
            raise FileNotFoundError(self.file)
        if not self.file.is_file():
            raise OSError(f"{self.file} is not a file")
        assert self.file.read_bytes().startswith(
            b"BCDB "
        ), f"invalid table file {self.file}: doesn't start with BCDB"

    @functools.cached_property
    def name(self) -> str:
        """
        The name of the table (`self.file.stem`).

        Returns:
            str: The name of the table.
        """
        return self.file.stem

    @functools.cached_property
    def attributes(self) -> list["Attribute"]:
        """
        Get the attributes for the table.

        Raises:
            ValueError: if the table file is empty
            AssertionError: if the table file doesn't start with `BCDB `

            Other exceptions may be raised by other
            functions (Attribute.from_str) called by this function.

        Returns:
            list[Attribute]: The attributes
        """
        first_line = None
        with self.lock:
            with self.file.open("r", encoding="utf-8") as file:
                for line in file:
                    first_line = line
                    break
        assert first_line, "invalid table file: empty"
        assert first_line.startswith(
            "BCDB "
        ), "invalid table file: doesn't start with BCDB"
        return [
            Attribute.from_str(string, self)
            for string in first_line.removeprefix("BCDB ").split(";;")
        ]

    def get_attribute(self, name: str) -> "Attribute":
        """
        Get an attribute with name `name`.

        Args:
            name (str): The attribute's name

        Raises:
            AssertionError: if the attribute is not found

        Returns:
            Attribute: The attribute with name `name`
        """
        for attr in self.attributes:
            if attr.name == name:
                return attr
        raise AssertionError(
            f"invalid attribute with name {name}: doesn't exist"
        )

    def get_rows(self) -> list[tuple[Any, ...]]:
        """
        Get all rows in the table.

        Raises:
            AssertionError: if the table file doesn't start with `BCDB `
            AssertionError: if there are too many columns on a row
            AssertionError: if there are not enough columns on a row

            Other exceptions may be raised by other functions
            (Attribute.convert_and_verify) called by this function.

        Returns:
            list[tuple[Any, ...]]: All rows in the table. This is a list of
            tuples. The tuples represent rows. All of the tuples should have
            the same length.
        """
        with self.lock:
            contents = self.file.read_text(encoding="utf-8")
        assert contents.startswith(
            "BCDB "
        ), "invalid table file: doesn't start with BCDB"
        rows = contents.splitlines()
        rows.pop(0)  # the BCDB... line
        rv: list[tuple[Any, ...]] = []
        column_: list[Any] = []
        #                      the 1st line is BCDB...
        #                                v
        for rownum, row in enumerate(rows, 2):
            for columnnum, column in enumerate(row.split(";;")):
                if columnnum >= len(self.attributes):
                    raise AssertionError(
                        f"invalid table file: too many columns on row {rownum}"
                    )
                attr = self.attributes[columnnum]
                column_.append(attr.convert_and_verify(column))
            if len(column_) != len(self.attributes):
                raise AssertionError(
                    f"invalid table file: invalid columns on row {rownum},"
                    f" expected {len(self.attributes)}, got {len(column_)}"
                )
            rv.append(tuple(column_))
            column_ = []
        return rv

    def add_row(self, row: tuple[Any, ...]) -> None:
        """
        Add a row to the table.

        Raises:
            AssertionError: if the number of columns in `row` is not correct

            Other exceptions may be raised by other
            functions (Attribute.verify_before_writing) called by this
            function.

        Args:
            row (tuple[Any, ...]): The row. Each element in the tuple
            represents a column.
        """
        assert len(row) == len(self.attributes), (
            f"invalid value for table {self.name}: invalid number of columns,"
            f" expected {len(self.attributes)}, got {len(row)}"
        )
        for idx, column in enumerate(row):
            attr = self.attributes[idx]
            attr.verify_before_writing(column)
        with self.lock:
            with self.file.open("a", encoding="utf-8") as file:
                file.write(f"{';;'.join(str(column) for column in row)}\n")

    def verify_from(self, obj: Any, attribute: "str | Attribute") -> None:
        """
        Verify attribute.from_ (".from_", "from_", "from").

        Args:
            obj (Any): The object to check.
            attribute (str | Attribute): The attribute or its name.

        Raises:
            AssertionError: if the other table (with the name
            `attribute.from_`) doesn't have an attribute called
            `attribute.name`
            AssertionError: if a row doesn't have a column with that
            attribute (corrupted table file?)
            AssertionError: if `obj` doesn't exist in the other table

            Other exceptions may be raised by other
            functions (get_attribute, Attribute.check_from, __post_init__,
            get_rows) called by this function.
        """
        if isinstance(attribute, str):
            attribute = self.get_attribute(attribute)
        if not attribute.from_:
            return
        table_file = attribute.check_from()
        attr_idx = self.attributes.index(attribute)
        from_table = self.__class__(table_file)
        from_table.get_attribute(attribute.name)
        for idx, row in enumerate(from_table.get_rows(), 2):
            try:
                if row[attr_idx] == obj:
                    break
            except IndexError as exc:  # pragma: no cover
                raise AssertionError(
                    f"invalid table file: no column {attr_idx + 1} at row"
                    f" {idx}"
                ) from exc
        else:  # no break
            raise AssertionError(
                f"invalid value for attribute {attribute.name}: {obj!r} does"
                f" not exist in from table {from_table.name}"
            )

    def verify_requirements(self, attribute: "Attribute", obj: Any) -> None:
        """
        Verify that the requirements are met.

        Args:
            attribute (Attribute): The attribute. Its `.requirements` attribute
            will be used.
            obj (Any): The object to check.

        Raises:
            AssertionError: if the attribute cannot be found in the table file
            AssertionError: if the attribute is supposed to be unique, but
            it's already used
            AssertionError: if the row doesn't have that column
            AssertionError: if the requirements are invalid
        """
        # sourcery skip: swap-if-else-branches
        if (isinstance(obj, str)) and (";;" in obj):
            raise AssertionError(
                f"invalid value at attribute {attribute.name}: string contains"
                " separator"
            )
        try:
            which_column = self.attributes.index(attribute)
        except ValueError as exc:
            raise AssertionError(
                f"invalid table file: cannot find attribute {attribute.name}"
                f" within table {self.name}"
            ) from exc
        if attribute.requirements == AttributeRequirements.UNIQUE:
            for idx, row in enumerate(self.get_rows()):
                try:
                    if row[which_column] == obj:
                        raise AssertionError(
                            f"invalid value at attribute {attribute.name}:"
                            " attribute is unique, but it already appears on"
                            f" row {idx + 2}"
                        )
                except IndexError as exc:  # pragma: no cover
                    raise AssertionError(
                        f"invalid table file: row {idx + 2} doesn't have"
                        f" column {which_column + 1}"
                    ) from exc
        else:  # pragma: no cover
            raise AssertionError(
                f"invalid table file: unknown requirement"
                f" {attribute.requirements!r}"
            )


class AttributeType(enum.StrEnum):
    """StrEnum for attribute types."""

    BOOLEAN = "BOOLEAN"
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    STRING = "STRING"
    # if you modify these don't forget to change Attribute.convert_and_verify,
    # and Attribute.verify_before_writing


class AttributeRequirements(enum.StrEnum):
    """StrEnum for attribute requirements."""

    UNIQUE = "UNIQUE"
    # if you modify these don't forget to change Table.verify_requirements


@dataclasses.dataclass
class Attribute:
    """
    An attribute.

    Args:
        name (str): The name of the attribute
        type_ (AttributeType): The type of the attribute
        requirements (AttributeRequirements | None): The requirements for the
        attribute, or None. Defaults to None.
        from_ (str | None): The table's name where this attribute is from or
        None. Defaults to None.
    """

    name: str
    type_: AttributeType
    requirements: AttributeRequirements | None = None
    # ^^ if you modify requirements don't forget to change
    #    verify_before_writing
    from_: str | None = None
    # if you modify these don't forget to change to_str and from_str

    table: Table | None = dataclasses.field(
        default=None, kw_only=True, compare=False
    )

    def __post_init__(self) -> None:
        assert (
            ";;" not in self.name
        ), "invalid attribute name: string contains separator"
        assert isinstance(
            self.type_, AttributeType
        ), "invalid attribute type: must be instance of AttributeType"
        assert (self.requirements is None) or (
            isinstance(self.requirements, AttributeRequirements)
        ), (
            "invalid attribute requirements: must be None or instance of"
            " AttributeRequirements"
        )
        if self.table and self.from_:
            self.check_from()

    def check_from(self) -> pathlib.Path:
        """
        Check self.from_ ("from", ".from_", "from_").
        [WARNING] `self.table` and `self.from_` MUST exist

        Raises:
            AssertionError: if `.from_` doesn't exist
            AssertionError: if `.table` doesn't exist
            AssertionError: if `from` is self
            AssertionError: if `from` doesn't exist

        Returns:
            pathlib.Path: the table file for `from`
        """
        assert self.from_, (
            "invalid attribute: doesn't have .from_ (check_from requires"
            " .from_)"
        )
        assert self.table, (
            "invalid attribute: doesn't have .table (check_from requires"
            " .table)"
        )
        assert (
            self.from_ != self.table.name
        ), "invalid attribute from: from cannot be this table"
        # checking if it even exists
        dbdir = self.table.file.parent
        file = dbdir / self.from_
        assert (
            file
        ).exists(), (
            f"invalid attribute from: table {self.from_!r} doesn't exist"
        )
        return dbdir / self.from_

    def to_str(self) -> str:
        """
        Convert the attribute (`self`) to a string.

        Returns:
            str: The string representation of the attribute.
        """
        return f"{self.name} {self.type_} {self.requirements} {self.from_}"

    @classmethod
    def from_str(cls, string: str, table: Table) -> Self:  # type: ignore
        """
        Convert the string got from `.to_str()` to an attribute.

        Raises:
            AssertionError: if the string doesn't have the expected number of
            columns

        Args:
            string (str): The string got from `.to_str()`
            table (Table): The table that the attribute is from

        Returns:
            Self: The new attribute
        """
        parts = string.strip().split(" ")
        assert (
            len(parts) == 4
        ), f"invalid syntax for attribute: not 4 parts/columns, {len(parts)}"
        name = parts[0]
        type_ = AttributeType(parts[1])
        requirements = (
            None if parts[2] == "None" else AttributeRequirements(parts[2])
        )
        from_ = None if parts[3] == "None" else parts[3]
        return cls(name, type_, requirements, from_, table=table)

    def convert_and_verify(self, string: str) -> Any:
        """
        Convert the string got from the table file to a python object while
        verifying it. Requirements (UNIQUE) are NOT verified.

        Args:
            string (str): The string got from the table file.

        Raises:
            AssertionError: if the object is supposed to be boolean, but it
            isn't
            AssertionError: if the object is supposed to be float, but it
            isn't
            AssertionError: if the object is supposed to be integer, but it
            isn't
            AssertionError: if the type is unknown

            Other exceptions may be raised by other functions (float(), int())
            called by this function.

        Returns:
            Any: The converted object.
        """
        assert ";;" not in string, (
            f"invalid value at attribute {self.name}: string contains"
            " separator"
        )
        if self.type_ == AttributeType.BOOLEAN:
            if string.lower() == "true":
                return True
            if string.lower() == "false":
                return False
            raise AssertionError(
                f"invalid table file: {string!r} isn't boolean"
            )
        if self.type_ == AttributeType.FLOAT:
            assert "." in string, f"invalid table file: {string!r} isn't float"
            return float(string)
        if self.type_ == AttributeType.INTEGER:
            assert (
                "." not in string
            ), f"invalid table file: {string!r} isn't integer"
            return int(string)
        if self.type_ == AttributeType.STRING:
            return string
        raise AssertionError(  # pragma: no cover
            f"invalid attribute: unknown attribute type {self.type_!r}"
        )

    def verify_before_writing(self, obj: Any) -> None:
        """
        Verify that `obj` can be written to the table file. This checks the
        types, and requirements (UNIQUE), and from.

        Args:
            obj (Any): The object to check.

        Raises:
            AssertionError: if the object is supposed to be boolean, but it
            isn't
            AssertionError: if the object is supposed to be float, but it
            isn't
            AssertionError: if the object is supposed to be integer, but it
            isn't
            AssertionError: if the object is supposed to be string, but it
            isn't

            Other exceptions may be raised by other
            functions (Table.verify_requirements) called by this function.
        """
        if self.type_ == AttributeType.BOOLEAN:
            assert isinstance(
                obj, bool
            ), f"invalid value at attribute {self.name}: invalid boolean"
        elif self.type_ == AttributeType.FLOAT:
            assert isinstance(
                obj, float
            ), f"invalid value at attribute {self.name}: invalid float"
        elif self.type_ == AttributeType.INTEGER:
            assert isinstance(
                obj, int
            ), f"invalid value at attribute {self.name}: invalid integer"
        elif self.type_ == AttributeType.STRING:
            assert isinstance(
                obj, str
            ), f"invalid value at attribute {self.name}: invalid string"
            assert ";;" not in obj, (
                f"invalid value at attribute {self.name}: string contains"
                " separator"
            )
        else:  # pragma: no cover
            raise AssertionError(
                f"invalid attribute: unknown attribute type {self.type_!r}"
            )
        if self.requirements:
            assert self.table, "invalid attribute: doesn't have .table"
            self.table.verify_requirements(self, obj)
        if self.from_ and self.table:
            self.table.verify_from(obj, self)


@dataclasses.dataclass(order=True, slots=True)
class Database:
    """
    The database class.

    Args:
        directory (pathlib.Path | str): The database directory. The table
        files will be put in this directory.
    """

    directory: pathlib.Path | str

    def __post_init__(self) -> None:
        if isinstance(self.directory, str):
            self.directory = pathlib.Path(self.directory)
        if not self.directory.exists():
            raise FileNotFoundError(self.directory)
        if not self.directory.is_dir():
            raise NotADirectoryError(self.directory)
        assert (
            ";;" not in self.directory.resolve().__fspath__()
        ), "invalid database directory: path contains separator"

    @property
    def tables(self) -> list[Table]:
        """
        Returns all of the tables in the database.

        Returns:
            list[Table]: A list of tables in the database.
        """
        assert isinstance(self.directory, pathlib.Path)
        return [Table(file) for file in self.directory.iterdir()]

    def add_table(
        self, table_name: str, table_attributes: list[Attribute]
    ) -> Table:
        """
        Add a table to the database.

        Raises:
            AssertionError: If the table's name is invalid
            AssertionError: If the table's attributes is invalid
            AssertionError: If the table's path already exists

            Other exceptions may be raised by other
            functions (Table.__post_init__) called by this function.

        Args:
            table_name (str): The table's name. Must only consist of
            letters and digits `[a-zA-Z0-9]`
            table_attributes (list[Attribute]): The table's attributes.

        Returns:
            Table: The new table.
        """
        assert isinstance(self.directory, pathlib.Path)
        assert all(
            val in stringlib.ascii_letters + stringlib.digits
            for val in table_name
        ), f"invalid table name: {table_name!r}"
        assert all(isinstance(val, Attribute) for val in table_attributes), (
            "invalid table attribute: must be a list of Attribute, got"
            f" {table_attributes!r}"
        )
        assert table_attributes, "invalid table attributes: empty"
        table_path = self.directory / table_name
        assert not table_path.exists(), "table with that name already exists"
        table_path.write_text(
            f"BCDB {';;'.join(attr.to_str() for attr in table_attributes)}\n"
        )
        table = Table(table_path)
        for attr in table_attributes:
            attr.table = table
        return table

    def get_table(self, table_name: str) -> Table:
        """
        Get the table with the name `table_name`.

        Args:
            table_name (str): The table's name to return.

        Raises:
            AssertionError: If the table isn't found.

        Returns:
            Table: The table.
        """
        for table in self.tables:
            if table_name.casefold() == table.name.casefold():
                return table
        raise AssertionError(
            f"invalid table: no table found with name {table_name!r}"
        )

    def remove_table(self, table_name: str) -> None:
        """
        Remove the table.

        Raises:
            AssertionError: if `table_name` is invalid
            AssertionError: if table with that name doesn't exist

        Args:
            table_name (str): The table's name to remove.
        """
        assert isinstance(self.directory, pathlib.Path)
        assert all(
            val in stringlib.ascii_letters + stringlib.digits
            for val in table_name
        ), f"invalid table name: {table_name!r}"
        table_path = self.directory / table_name
        assert table_path.exists(), "table with that name doesn't exist"
        table_path.unlink(missing_ok=False)
