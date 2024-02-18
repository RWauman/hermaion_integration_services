import polars as pl

from dataclasses import dataclass, field
from uuid import uuid1, UUID

from collections import OrderedDict

from exceptions import ExtensionMissmatch, FileTypeNotSupported


SCAN_METHODS = {
    'csv': pl.scan_csv,
    'parquet': pl.scan_parquet,
}

@dataclass
class FileConnector:
    name: str
    type: str
    path: str
    _id: UUID = field(init=False,  default_factory=uuid1)
    _reader: callable = field(init=False)
    _schema: OrderedDict = field(init=False)
    _columns: tuple = field(init=False)

    def __post_init__(self):
        """
        Perform post-initialization tasks for the object.

        This method initializes the reader function based on the specified file type,
        tests the connection, and sets up the execution plan, schema, and columns.

        """
        self._reader = SCAN_METHODS.get(self.type)
        if self.test_connection():
            self._execution_plan = self._reader(self.path)
            self._schema = self._execution_plan.schema
            self._columns = tuple(self._schema.keys())

    def test_connection(self) -> bool|Exception:
        """
        Test the connection to the file and return True if successful, otherwise return the FileNotFoundError exception.
        """
        try:
            return True
        except FileNotFoundError as e:
            return e

    def show_sample(self, n_rows: int = 100) -> pl.DataFrame:
        """
        Fetch the first n_rows of the file(s) and return a DataFrame.
        As we use fetch method and not head method, you can expect more rows to be retrieved than n_rows

        Args:
            n_rows (int, optional): number of row to fetch. Defaults to 100.

        Returns:
            pl.DataFrame: DataFrame containing the first fetched rows of the file(s). 
        """
        return self._execution_plan.fetch(n_rows)
    
    def select_columns(self, columns: list|tuple) -> pl.LazyFrame:
        """
        Selects the specified columns from the execution plan.

        Args:
            columns (list|tuple): A list or tuple of column names to select.

        Returns:
            pl.LazyFrame: The modified execution plan with the selected columns.
        """
        self._execution_plan = self._execution_plan.select(columns)
    
    def rename_columns(self, new_names: dict) -> pl.LazyFrame:
        """
        Renames the columns of the LazyFrame using the provided dictionary of new names.
        
        Args:
            new_names (dict): A dictionary mapping old column names to new column names.
        
        Returns:
            pl.LazyFrame: The modified execution plan with renamed columns.
        """
        self._execution_plan =  self._reader(self.path).rename(new_names)

    def cast(self, new_cast: dict) -> pl.LazyFrame:
        """
        Casts the columns of the LazyFrame to the specified data types.

        Args:
            new_cast (dict): A dictionary mapping column names to their new data types.

        Returns:
            pl.LazyFrame: The modified execution plan the columns casted to the specified data types.
        """
        self._execution_plan = self._reader(self.path).cast(new_cast)

    def get_output(self) -> pl.LazyFrame:
        """
        Returns the execution plan.

        Returns:
            pl.LazyFrame: The execution plan.
        """
        return self._execution_plan

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, value: str):
        self._name = value

    @property
    def type(self) -> str:
        return self._type

    @type.setter
    def type(self, value: str):
        """
        Set the type of the object.
        
        Args:
            value (str): The type to be set.
        
        Raises:
            FileTypeNotSupported: If the provided type is not supported.
        """
        if value not in SCAN_METHODS.keys():
            raise FileTypeNotSupported
        self._type = value

    @property
    def path(self) -> str:
        return self._path

    @path.setter
    def path(self, value: str):
        """
        Set the path for the object.

        Args:
            value (str): The path to set.

        Raises:
            ExtensionMissmatch: If the path does not have the correct extension.
        """
        if value.endswith('.'+self.type):
            self._path = value
        else:
            raise ExtensionMissmatch