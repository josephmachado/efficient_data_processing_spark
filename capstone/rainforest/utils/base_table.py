from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, Type

from pyspark.sql import DataFrame


@dataclass
class ETLDataSet:
    name: str
    curr_data: DataFrame
    primary_keys: List[str]
    storage_path: str
    data_format: str
    database: str
    partition_keys: List[str]


class TableETL(ABC):
    @abstractmethod
    def __init__(
        self,
        spark,
        upstream_table_names: Optional[List[Type[TableETL]]],
        name: str,
        primary_keys: List[str],
        storage_path: str,
        data_format: str,
        database: str,
        partition_keys: List[str],
        run_upstream: bool = True,
    ) -> None:
        self.spark = spark
        self.upstream_table_names = upstream_table_names
        self.name = name
        self.primary_keys = primary_keys
        self.storage_path = storage_path
        self.data_format = data_format
        self.database = database
        self.partition_keys = partition_keys
        self.run_upstream = run_upstream

    @abstractmethod
    def extract_upstream(self) -> List[ETLDataSet]:
        pass

    @abstractmethod
    def transform_upstream(
        self, upstream_datasets: List[ETLDataSet]
    ) -> ETLDataSet:
        pass

    @abstractmethod
    def validate(self, data: ETLDataSet) -> bool:
        pass

    @abstractmethod
    def load(self, data: ETLDataSet) -> None:
        pass

    def run(self) -> None:
        transformed_data = self.transform_upstream(self.extract_upstream())
        self.validate(transformed_data)
        self.load(transformed_data)

    @abstractmethod
    def read(self, partition_values: Optional[Dict[str, str]] = None) -> ETLDataSet:
        pass
