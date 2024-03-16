from dataclasses import dataclass
from abc import ABC, abstractmethod

from typing import Dict, Type, List, Optional
from pyspark.sql import DataFrame
from __future__ import annotations


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
        upstream_table_names: Optional[List[Type[TableETL]]],
        name: str,
        primary_keys: List[str],
        storage_path: str,
        data_format: str,
        database: str,
        partition_keys: List[str],
    ) -> None:
        pass

    @abstractmethod
    def extract_upstream(self, run_upstream: bool = True) -> List[ETLDataSet]:
        pass

    @abstractmethod
    def transform_upstream(self, upstream_datasets: List[ETLDataSet]) -> ETLDataSet:
        pass

    @abstractmethod
    def validate(self, data: ETLDataSet) -> bool:
        pass

    @abstractmethod
    def load(self, data: ETLDataSet) -> None:
        pass

    def run(self, run_upstream: bool = True) -> None:
        self.load(
            self.validate(self.transform_upstream(self.extract_upstream(run_upstream)))
        )

    @abstractmethod
    def read(self, partition_keys: Optional[List[str]] = None) -> ETLDataSet:
        pass
