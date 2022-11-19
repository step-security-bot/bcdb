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
# pylint: disable=missing-function-docstring,missing-class-docstring
# pylint: disable=redefined-outer-name
#                 ^^^^^^^^^^^^^^^^^^^^ for fixtures
import pathlib
import secrets

import pytest

import bcdb


@pytest.fixture
def file(tmp_path: pathlib.Path) -> pathlib.Path:
    file_ = tmp_path / secrets.token_hex(8)
    file_.touch()
    return file_


@pytest.fixture
def table_file(file: pathlib.Path) -> pathlib.Path:
    file.write_text("BCDB testattr STRING UNIQUE None")
    return file


class TestTable:
    @staticmethod
    def test_post_init_good(table_file: pathlib.Path) -> None:
        bcdb.Table(table_file)

    @staticmethod
    def test_post_init_bad_notexists(tmp_path: pathlib.Path) -> None:
        with pytest.raises(
            FileNotFoundError, match="file_that_does_not_exist"
        ):
            bcdb.Table(tmp_path / "file_that_does_not_exist")

    @staticmethod
    def test_post_init_bad_notfile(tmp_path: pathlib.Path) -> None:
        with pytest.raises(OSError, match=r"is not a file"):
            bcdb.Table(tmp_path)

    @staticmethod
    def test_post_init_bad_nottablefile(file: pathlib.Path) -> None:
        file.write_bytes(b"Hello, World!")
        with pytest.raises(AssertionError, match=r"doesn't start with BCDB"):
            bcdb.Table(file)

    @staticmethod
    def test_name(table_file: pathlib.Path) -> None:
        assert bcdb.Table(table_file).name == table_file.stem

    @staticmethod
    def test_attributes_good(tmp_path: pathlib.Path) -> None:
        db = bcdb.Database(tmp_path)
        attrs = [
            bcdb.Attribute(
                "testattr1",
                bcdb.AttributeType.BOOLEAN,
            ),
            bcdb.Attribute(
                "testattr2",
                bcdb.AttributeType.FLOAT,
            ),
            bcdb.Attribute(
                "testattr3",
                bcdb.AttributeType.INTEGER,
            ),
            bcdb.Attribute(
                "testattr4",
                bcdb.AttributeType.STRING,
                bcdb.AttributeRequirements.UNIQUE,
            ),
        ]
        table = db.add_table("table", attrs)
        assert table.attributes == attrs

    @staticmethod
    def test_attributes_bad_empty(tmp_path: pathlib.Path) -> None:
        db = bcdb.Database(tmp_path)
        attrs = [
            bcdb.Attribute(
                "testattr1",
                bcdb.AttributeType.BOOLEAN,
            ),
            bcdb.Attribute(
                "testattr2",
                bcdb.AttributeType.FLOAT,
            ),
            bcdb.Attribute(
                "testattr3",
                bcdb.AttributeType.INTEGER,
            ),
            bcdb.Attribute(
                "testattr4",
                bcdb.AttributeType.STRING,
                bcdb.AttributeRequirements.UNIQUE,
            ),
        ]
        table = db.add_table("table", attrs)
        (tmp_path / "table").write_bytes(b"")
        with pytest.raises(AssertionError, match=r"empty"):
            table.attributes  # pylint: disable=pointless-statement

    @staticmethod
    def test_attributes_bad_nottablefile(tmp_path: pathlib.Path) -> None:
        db = bcdb.Database(tmp_path)
        attrs = [
            bcdb.Attribute(
                "testattr1",
                bcdb.AttributeType.BOOLEAN,
            ),
            bcdb.Attribute(
                "testattr2",
                bcdb.AttributeType.FLOAT,
            ),
            bcdb.Attribute(
                "testattr3",
                bcdb.AttributeType.INTEGER,
            ),
            bcdb.Attribute(
                "testattr4",
                bcdb.AttributeType.STRING,
                bcdb.AttributeRequirements.UNIQUE,
            ),
        ]
        table = db.add_table("table", attrs)
        (tmp_path / "table").write_bytes(b"Hello, World!")
        with pytest.raises(AssertionError, match=r"doesn't start with BCDB"):
            table.attributes  # pylint: disable=pointless-statement

    @staticmethod
    def test_get_attribute(tmp_path: pathlib.Path) -> None:
        db = bcdb.Database(tmp_path)
        attr = bcdb.Attribute("attr", bcdb.AttributeType.FLOAT)
        table = db.add_table("table", [attr])
        assert table.get_attribute("attr") == attr
        with pytest.raises(AssertionError, match=r"doesn't exist"):
            table.get_attribute("notattr")

    @staticmethod
    def test_verify_from(tmp_path: pathlib.Path) -> None:
        db = bcdb.Database(tmp_path)
        table1 = db.add_table(
            "t1",
            [
                bcdb.Attribute(
                    "user_id",
                    bcdb.AttributeType.INTEGER,
                    bcdb.AttributeRequirements.UNIQUE,
                ),
            ],
        )
        table2 = db.add_table(
            "t2",
            [
                bcdb.Attribute(
                    "user_id",
                    bcdb.AttributeType.INTEGER,
                    bcdb.AttributeRequirements.UNIQUE,
                    "t1",
                ),
            ],
        )
        table1.verify_from(
            1,
            bcdb.Attribute(
                "user_id",
                bcdb.AttributeType.INTEGER,
                bcdb.AttributeRequirements.UNIQUE,
            ),
        )
        table1.add_row((3141592653,))
        table2.add_row((3141592653,))
        with pytest.raises(
            AssertionError,
            match=r"12648430 does not exist in from table t1",
        ):
            table2.add_row((12648430,))

    @staticmethod
    def test_verify_from_bad(tmp_path: pathlib.Path) -> None:
        db = bcdb.Database(tmp_path)
        db.add_table(
            "t1",
            [
                bcdb.Attribute(
                    "user_id",
                    bcdb.AttributeType.INTEGER,
                    bcdb.AttributeRequirements.UNIQUE,
                ),
            ],
        )
        table2 = db.add_table(
            "t2",
            [
                bcdb.Attribute(
                    "user_id",
                    bcdb.AttributeType.INTEGER,
                    bcdb.AttributeRequirements.UNIQUE,
                    "t1",
                ),
            ],
        )
        with pytest.raises(
            AssertionError,
            match=r"1 does not exist in from table t1",
        ):
            table2.verify_from(1, "user_id")

    @staticmethod
    def test_get_and_add_rows(tmp_path: pathlib.Path) -> None:
        db = bcdb.Database(tmp_path)
        attrs = [
            bcdb.Attribute(
                "testattr1",
                bcdb.AttributeType.BOOLEAN,
            ),
            bcdb.Attribute(
                "testattr2",
                bcdb.AttributeType.FLOAT,
            ),
            bcdb.Attribute(
                "testattr3",
                bcdb.AttributeType.INTEGER,
            ),
            bcdb.Attribute(
                "testattr4",
                bcdb.AttributeType.STRING,
                bcdb.AttributeRequirements.UNIQUE,
            ),
        ]
        table = db.add_table("table", attrs)
        rows: list[tuple[object, ...]] = [
            (True, -0.31, -15, "Hello, World!"),
            (False, 2147.654, -98372, "The quick brown fox"),
            (False, -34987.145, 1236745, "jumps over the"),
            (True, 23546.0, 256, "lazy dog."),
            (False, 0.0, 0, ""),
        ]
        for row in rows:
            table.add_row(row)  # checking add_row
        assert table.get_rows() == rows  # checking get_rows

    @staticmethod
    def test_get_rows_notenough(tmp_path: pathlib.Path) -> None:
        db = bcdb.Database(tmp_path)
        attrs = [
            bcdb.Attribute(
                "testattr1",
                bcdb.AttributeType.BOOLEAN,
            ),
            bcdb.Attribute(
                "testattr2",
                bcdb.AttributeType.FLOAT,
            ),
        ]
        table = db.add_table("table", attrs)
        with table.file.open("a", encoding="utf-8") as file:
            file.write("false")
        with pytest.raises(
            AssertionError,
            match=r"invalid columns on row 2, expected 2, got 1",
        ):
            table.get_rows()

    @staticmethod
    def test_get_rows_toomuch(tmp_path: pathlib.Path) -> None:
        db = bcdb.Database(tmp_path)
        attrs = [
            bcdb.Attribute(
                "testattr1",
                bcdb.AttributeType.BOOLEAN,
            ),
        ]
        table = db.add_table("table", attrs)
        with table.file.open("w", encoding="utf-8") as file:
            file.write("BCDB a BOOLEAN None None\ntrue;;3.14;;Hello world")
        with pytest.raises(AssertionError, match=r"too many columns on row 2"):
            table.get_rows()

    @staticmethod
    def test_remove_rows(tmp_path: pathlib.Path) -> None:
        db = bcdb.Database(tmp_path)
        table = db.add_table(
            "table", [bcdb.Attribute("testattr1", bcdb.AttributeType.BOOLEAN)]
        )
        table.add_row((True,))
        table.add_row((False,))
        table.add_row((True,))
        table.add_row((True,))
        table.add_row((False,))
        table.add_row((False,))
        table.add_row((True,))
        table.add_row((False,))
        table.remove_rows(lambda t: t[0])
        rows = table.get_rows()
        assert len(rows) == 4
        assert all(row == (False,) for row in rows)

    @staticmethod
    def test_remove_rows_limit(tmp_path: pathlib.Path) -> None:
        db = bcdb.Database(tmp_path)
        table = db.add_table(
            "table", [bcdb.Attribute("testattr1", bcdb.AttributeType.BOOLEAN)]
        )
        with pytest.raises(
            AssertionError,
            match=r"exceeded the limit \(-1\) with 0 number of rows removed\.",
        ):
            table.remove_rows(lambda _: True, limit=-1)

    @staticmethod
    def test_verify_requirements(tmp_path: pathlib.Path) -> None:
        db = bcdb.Database(tmp_path)
        attrs = [
            bcdb.Attribute(
                "testattr1",
                bcdb.AttributeType.BOOLEAN,
                bcdb.AttributeRequirements.UNIQUE,
            ),
            bcdb.Attribute(
                "testattr2",
                bcdb.AttributeType.FLOAT,
                bcdb.AttributeRequirements.UNIQUE,
            ),
            bcdb.Attribute(
                "testattr3",
                bcdb.AttributeType.INTEGER,
                bcdb.AttributeRequirements.UNIQUE,
            ),
            bcdb.Attribute(
                "testattr4",
                bcdb.AttributeType.STRING,
                bcdb.AttributeRequirements.UNIQUE,
            ),
        ]
        table = db.add_table("table", attrs)

        with pytest.raises(AssertionError, match=r"string contains separator"):
            table.verify_requirements(attrs[0], "not an;;injection")

        attr_that_doesnt_exist = bcdb.Attribute(
            "testattr5",
            bcdb.AttributeType.BOOLEAN,
        )
        with pytest.raises(
            AssertionError,
            match=r"cannot find attribute testattr5 within table table",
        ):
            table.verify_requirements(attr_that_doesnt_exist, False)

        table.add_row((True, 3.14, 6, "hewwo world~"))

        table.add_row((False, 1.23, 9, "UwU"))

        with pytest.raises(
            AssertionError,
            match=r"attribute is unique, but it already appears on row 2",
        ):
            table.add_row((True, 2.34, 10, "the quick bwown fox"))

        with pytest.raises(
            AssertionError,
            match=r"attribute is unique, but it already appears on row 3",
        ):
            table.add_row((False, 4.45, 11, "jumps ovew the"))

        with pytest.raises(
            AssertionError,
            match=r"attribute is unique, but it already appears on row 2",
        ):
            table.add_row((True, 3.14, 6, "hewwo world~"))


class TestAttribute:
    @staticmethod
    def test_post_init_good() -> None:
        bcdb.Attribute("helloworld", bcdb.AttributeType.INTEGER)
        bcdb.Attribute("helloworld", bcdb.AttributeType.STRING, None)
        bcdb.Attribute(
            "helloworld",
            bcdb.AttributeType.BOOLEAN,
            bcdb.AttributeRequirements.UNIQUE,
        )
        bcdb.Attribute(
            "helloworld", bcdb.AttributeType.STRING, None, "othertable"
        )
        bcdb.Attribute(
            "helloworld",
            bcdb.AttributeType.BOOLEAN,
            bcdb.AttributeRequirements.UNIQUE,
            "othertable",
        )

    @staticmethod
    def test_post_init_bad() -> None:
        with pytest.raises(AssertionError):
            bcdb.Attribute(";;", bcdb.AttributeType.BOOLEAN)
        with pytest.raises(AssertionError):
            bcdb.Attribute("goodname", "something else")  # type: ignore
        with pytest.raises(AssertionError):
            bcdb.Attribute(
                "goodname",
                bcdb.AttributeType.BOOLEAN,
                "something else",  # type: ignore
            )

    @staticmethod
    def test_check_from(tmp_path: pathlib.Path) -> None:
        db = bcdb.Database(tmp_path)
        db.add_table(
            "t1", [bcdb.Attribute("attr", bcdb.AttributeType.INTEGER)]
        )
        table2 = db.add_table(
            "t2",
            [bcdb.Attribute("attr", bcdb.AttributeType.INTEGER, from_="t1")],
        )
        table2.attributes[0].check_from()

    @staticmethod
    def test_check_from_bad(tmp_path: pathlib.Path) -> None:
        with pytest.raises(AssertionError):
            bcdb.Attribute("attr", bcdb.AttributeType.INTEGER).check_from()
        with pytest.raises(AssertionError):
            bcdb.Attribute(
                "attr", bcdb.AttributeType.INTEGER, from_="idk"
            ).check_from()
        db = bcdb.Database(tmp_path)
        attr = bcdb.Attribute("attr", bcdb.AttributeType.INTEGER, from_="t1")
        db.add_table(
            "t1",
            [attr],
        )
        with pytest.raises(AssertionError):
            db.tables[0].attributes[0].check_from()
        db.remove_table("t1")
        attr = bcdb.Attribute("attr", bcdb.AttributeType.INTEGER, from_="idk")
        db.add_table(
            "t1",
            [attr],
        )
        with pytest.raises(AssertionError):
            db.tables[0].attributes[0].check_from()

    @staticmethod
    def test_to_str() -> None:
        assert (
            bcdb.Attribute(
                "thename",
                bcdb.AttributeType.STRING,
                bcdb.AttributeRequirements.UNIQUE,
                "theothername",
            ).to_str()
            == "thename STRING UNIQUE theothername"
        )
        assert (
            bcdb.Attribute(
                "thename",
                bcdb.AttributeType.BOOLEAN,
            ).to_str()
            == "thename BOOLEAN None None"
        )

    @staticmethod
    def test_from_str() -> None:
        assert bcdb.Attribute.from_str(
            "thename STRING UNIQUE theothername", None  # type: ignore
        ) == bcdb.Attribute(
            "thename",
            bcdb.AttributeType.STRING,
            bcdb.AttributeRequirements.UNIQUE,
            "theothername",
        )
        assert bcdb.Attribute.from_str(
            "thename BOOLEAN None None", None  # type: ignore
        ) == bcdb.Attribute(
            "thename",
            bcdb.AttributeType.BOOLEAN,
        )

    @staticmethod
    def test_from_str_bad() -> None:
        with pytest.raises(AssertionError):
            bcdb.Attribute.from_str("hello STRING", None)  # type: ignore
        with pytest.raises(AssertionError):
            bcdb.Attribute.from_str(
                "hello STRING None None None",
                None,  # type: ignore
            )

    @staticmethod
    def test_convert_and_verify() -> None:
        attr = bcdb.Attribute("testattr", bcdb.AttributeType.BOOLEAN)
        assert attr.convert_and_verify("TrUE") is True
        assert attr.convert_and_verify("false") is False
        attr = bcdb.Attribute("testattr", bcdb.AttributeType.FLOAT)
        assert attr.convert_and_verify("3.14") == 3.14
        attr = bcdb.Attribute("testattr", bcdb.AttributeType.INTEGER)
        assert attr.convert_and_verify("-42") == -42
        attr = bcdb.Attribute("testattr", bcdb.AttributeType.STRING)
        assert attr.convert_and_verify("Hello, World!") == "Hello, World!"

    @staticmethod
    def test_convert_and_verify_bad() -> None:
        attr = bcdb.Attribute("testattr", bcdb.AttributeType.BOOLEAN)
        with pytest.raises(AssertionError):
            attr.convert_and_verify("totallynot;;injection")
        with pytest.raises(AssertionError):
            attr.convert_and_verify("Something else")
        attr = bcdb.Attribute("testattr", bcdb.AttributeType.FLOAT)
        with pytest.raises(AssertionError):
            attr.convert_and_verify("-9")
        with pytest.raises(AssertionError):
            attr.convert_and_verify("Something else")
        attr = bcdb.Attribute("testattr", bcdb.AttributeType.INTEGER)
        with pytest.raises(AssertionError):
            attr.convert_and_verify("5.0")
        with pytest.raises(ValueError):
            attr.convert_and_verify("Something else")

    @staticmethod
    def test_verify_before_writing(tmp_path: pathlib.Path) -> None:
        db = bcdb.Database(tmp_path)
        t1 = db.add_table(
            "t1",
            [
                bcdb.Attribute(
                    "attr",
                    bcdb.AttributeType.INTEGER,
                    bcdb.AttributeRequirements.UNIQUE,
                )
            ],
        )
        t2 = db.add_table(
            "t2",
            [
                bcdb.Attribute(
                    "attr",
                    bcdb.AttributeType.INTEGER,
                    bcdb.AttributeRequirements.UNIQUE,
                    "t1",
                ),
                bcdb.Attribute(
                    "attr2",
                    bcdb.AttributeType.STRING,
                    bcdb.AttributeRequirements.UNIQUE,
                ),
            ],
        )
        t1.attributes[0].verify_before_writing(3)
        t1.add_row((3,))
        with pytest.raises(AssertionError):
            t1.attributes[0].verify_before_writing(3)
        with pytest.raises(AssertionError):
            t1.attributes[0].verify_before_writing("3")

        t2.attributes[0].verify_before_writing(3)
        t2.attributes[1].verify_before_writing("asd")
        t2.add_row((3, "asd"))
        with pytest.raises(AssertionError):
            t2.attributes[0].verify_before_writing(4)
        with pytest.raises(AssertionError):
            t2.attributes[0].verify_before_writing("asd")
        t2.attributes[1].verify_before_writing("something else")


class TestDatabase:
    @staticmethod
    def test_post_init(tmp_path: pathlib.Path) -> None:
        bcdb.Database(tmp_path)

        assert bcdb.Database(str(tmp_path)).directory == tmp_path

    @staticmethod
    def test_post_init_bad(tmp_path: pathlib.Path, file: pathlib.Path) -> None:
        with pytest.raises(FileNotFoundError):
            bcdb.Database(tmp_path / "doesntexist")
        with pytest.raises(NotADirectoryError):
            bcdb.Database(file)
        try:
            dir_ = tmp_path / "fi;;le"
            dir_.mkdir()
        except Exception:  # pragma: no cover
            pytest.skip('OS doesn\'t allow ";;" in paths')

    @staticmethod
    def test_tables(tmp_path: pathlib.Path) -> None:
        db = bcdb.Database(tmp_path)
        t1 = db.add_table(
            "t1", [bcdb.Attribute("attr", bcdb.AttributeType.BOOLEAN)]
        )
        t2 = db.add_table(
            "t2", [bcdb.Attribute("attr", bcdb.AttributeType.FLOAT)]
        )
        t3 = db.add_table(
            "t3", [bcdb.Attribute("attr", bcdb.AttributeType.FLOAT)]
        )
        assert db.tables == [t1, t2, t3]

    @staticmethod
    def test_add_table(tmp_path: pathlib.Path) -> None:
        db = bcdb.Database(tmp_path)
        t1 = db.add_table(
            "t1",
            [
                bcdb.Attribute("attr1", bcdb.AttributeType.STRING, from_="t2"),
                bcdb.Attribute(
                    "attr2", bcdb.AttributeType.INTEGER, from_="t2"
                ),
            ],
        )
        t2 = db.add_table(
            "t2",
            [
                bcdb.Attribute("attr1", bcdb.AttributeType.STRING),
                bcdb.Attribute(
                    "attr2",
                    bcdb.AttributeType.INTEGER,
                    bcdb.AttributeRequirements.UNIQUE,
                ),
            ],
        )
        assert db.tables == [t1, t2]

    @staticmethod
    def test_add_table_bad(tmp_path: pathlib.Path) -> None:
        db = bcdb.Database(tmp_path)
        with pytest.raises(AssertionError):
            db.add_table("totallynot;;injection", [])
        with pytest.raises(AssertionError):
            db.add_table("hello world", [])
        with pytest.raises(AssertionError):
            db.add_table("howdy?", [])
        with pytest.raises(AssertionError):
            db.add_table("cool", [123])  # type: ignore
        with pytest.raises(AssertionError):
            db.add_table("cool", [])
        db.add_table("cool", [bcdb.Attribute("a", bcdb.AttributeType.FLOAT)])
        with pytest.raises(AssertionError):
            db.add_table(
                "cool", [bcdb.Attribute("b", bcdb.AttributeType.STRING)]
            )

    @staticmethod
    def test_get_table(tmp_path: pathlib.Path) -> None:
        db = bcdb.Database(tmp_path)
        t1 = db.add_table(
            "t1", [bcdb.Attribute("a", bcdb.AttributeType.FLOAT)]
        )

        assert db.get_table("t1") == t1
        with pytest.raises(AssertionError):
            db.get_table("t2")

    @staticmethod
    def test_remove_table(tmp_path: pathlib.Path) -> None:
        db = bcdb.Database(tmp_path)
        db.add_table("t1", [bcdb.Attribute("a", bcdb.AttributeType.FLOAT)])
        assert (db.directory / "t1").exists()
        db.remove_table("t1")
        assert not (db.directory / "t1").exists()

    @staticmethod
    def test_remove_table_bad(tmp_path: pathlib.Path) -> None:
        db = bcdb.Database(tmp_path)
        with pytest.raises(AssertionError):
            db.remove_table("totallynot;;injection")
        with pytest.raises(AssertionError):
            db.remove_table("t1")
