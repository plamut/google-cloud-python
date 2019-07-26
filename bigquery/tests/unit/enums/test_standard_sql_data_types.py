# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import pytest


@pytest.fixture
def module_under_test():
    from google.cloud.bigquery import enums

    return enums


@pytest.fixture
def enum_under_test():
    from google.cloud.bigquery.enums import StandardSqlDataTypes

    return StandardSqlDataTypes


@pytest.fixture
def gapic_enum():
    """The referential autogenerated enum the enum under test is based on."""
    from google.cloud.bigquery_v2.gapic.enums import StandardSqlDataType

    return StandardSqlDataType.TypeKind


def test_all_gapic_enum_members_are_known(module_under_test, gapic_enum):
    gapic_names = set(type_.name for type_ in gapic_enum)
    anticipated_names = (
        module_under_test._SQL_SCALAR_TYPES | module_under_test._SQL_NONSCALAR_TYPES
    )
    assert not (gapic_names - anticipated_names)  # no unhandled names


def test_standard_sql_types_enum_members(enum_under_test, gapic_enum):
    # check the presence of a few typical SQL types
    for name in ("INT64", "FLOAT64", "DATE", "BOOL", "GEOGRAPHY"):
        assert name in enum_under_test.__members__

    # the enum members must match those in the original gapic enum
    for member in enum_under_test:
        assert member.name in gapic_enum.__members__
        assert member.value == gapic_enum[member.name].value

    # check a few members that should *not* be copied over from the gapic enum
    for name in ("STRUCT", "ARRAY"):
        assert name in gapic_enum.__members__
        assert name not in enum_under_test.__members__


def test_standard_sql_types_enum_docstring(enum_under_test, gapic_enum):
    assert "STRUCT (int):" not in enum_under_test.__doc__
    assert "BOOL (int):" in enum_under_test.__doc__
    assert "TIME (int):" in enum_under_test.__doc__

    # all lines in the docstring should actually come from the original docstring
    doc_lines = enum_under_test.__doc__.splitlines()
    assert set(doc_lines) <= set(gapic_enum.__doc__.splitlines())
