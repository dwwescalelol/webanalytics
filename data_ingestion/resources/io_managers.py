from typing import Any
import polars as pl
from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
    UPathIOManager,
    _check as check,
)
from upath import UPath
from dagster._utils.cached_method import cached_method


class ParquetIOManager(UPathIOManager):
    """Translates between Polars DataFrames and CSVs on the local filesystem."""

    def __init__(self, base_path: UPath):
        self.base_path = base_path
        super().__init__(base_path)

    def load_from_path(self, context: InputContext, path: UPath):
        """This reads a dataframe from a CSV."""
        try:
            return pl.read_parquet(path.with_suffix(".parquet"))
        except:
            context.log.error(f"Failed to read parquet file at {path}")

    def dump_to_path(self, context: OutputContext, obj: pl.DataFrame, path: UPath):
        """This saves the dataframe as a CSV."""
        try:
            obj.write_parquet(path.with_suffix(".parquet"))
        except:
            context.log.error(f"Failed to write parquet file at {path}")


class LocalFileSystemIOManager(ConfigurableIOManager):

    base_path: str

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return True

    @cached_method
    def inner_io_manager(self) -> ParquetIOManager:
        return ParquetIOManager(base_path=UPath(self.base_path))

    def load_input(self, context: InputContext) -> Any:
        return self.inner_io_manager().load_input(context)

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        return self.inner_io_manager().handle_output(context, obj)
