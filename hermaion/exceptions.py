import polars as pl

SCAN_METHODS = {
    'csv': pl.scan_csv,
    'parquet': pl.scan_parquet,
}

class FileTypeNotSupported(Exception):
    """
    Exception raised when the provided file type is not supported.
    
    Attributes:
        message (str): The error message explaining the unsupported file types.
    """
    def __init__(self, message=f"File type provided is not supported. Only {','.join(SCAN_METHODS.keys())} are supported."):
        self.message = message
        super().__init__(self.message)

class ExtensionMissmatch(Exception):
    """
    Exception raised when the provided file's extension and file type does not match.
    
    Attributes:
        message (str): The error message explaining the missmatch.
    """
    def __init__(self, message="Provided file's extension and file type does not match."):
        self.message = message
        super().__init__(self.message)