from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


DEFAULT_NPARTITIONS = 10

def set_npartition_default(n):
    global DEFAULT_NPARTITIONS
    DEFAULT_NPARTITIONS = n


def get_npartitions():
    return DEFAULT_NPARTITIONS

from .dataframe import DataFrame
from .dataframe import from_pandas
from .dataframe import to_pandas
from .series import Series

from .io import (read_csv, read_parquet)

__all__ = ["DataFrame", "from_pandas", "to_pandas",
           "Series", "read_csv", "read_parquet"]
