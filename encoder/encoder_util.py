

from typing import Callable, List
from encoder.sample_entry import SampleEntry
from schema.sql.sql_table import SQLTable


class SampleBuffer:
    def __init__(self, sample_function: Callable[[], List[SampleEntry]]) -> None:
        self._sample_function = sample_function
        self._buffer = []

    def get_samples(self, sample_number: int) -> List[SampleEntry]:
        if sample_number > len(self._buffer):
            self._buffer = self._sample_function()
        samples = self._buffer[:sample_number]
        self._buffer = self._buffer[sample_number:]
        return samples


def get_samples_sql(cursor, table: SQLTable, sample_number: int) -> List[SampleEntry]:
    sample_query = sql_sample_query(table, sample_number)
    cursor.execute(sample_query)
    table_samples = []
    for sample_row in cursor.fetchall():
        table_samples.append(build_sql_sample(table, sample_row))
    return table_samples


def build_sql_sample(table: SQLTable, sample_row) -> SampleEntry:
    sample_values = {}
    for attribute, value in zip(table.attributes(), sample_row):
        sample_values[attribute] = value
    return SampleEntry([table], sample_values)


def sql_sample_query(table: SQLTable, sample_number: int) -> str:
    column_string = ", ".join([attribute.name() for attribute in table.attributes()])
    sample_query = "SELECT %s FROM %s ORDER BY RANDOM() LIMIT %d;" % (column_string, table.name(), sample_number)
    return sample_query

