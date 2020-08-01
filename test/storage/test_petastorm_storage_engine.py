# coding=utf-8
# Copyright 2018-2020 EVA
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import shutil
import unittest

import numpy as np
import pandas as pd

from src.catalog.models.df_metadata import DataFrameMetadata
from src.storage.petastorm_storage_engine import PetastormStorageEngine
from src.catalog.models.df_column import DataFrameColumn
from src.catalog.column_type import ColumnType
from src.models.storage.batch import Batch

NUM_FRAMES = 10


class PetastormStorageEngineTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table = None

    def create_dummy_batch(self, num_frames=NUM_FRAMES, filters=[]):
        if not filters:
            filters = range(num_frames)
        data = []
        for i in filters:
            data.append({'id': i,
                         'data': np.array(np.ones((2, 2, 3))
                                          * 0.1 * float(i + 1) * 255,
                                          dtype=np.uint8)})
        return Batch(pd.DataFrame(data))

    def create_sample_table(self):
        table_info = DataFrameMetadata("dataset", 'dataset')
        column_1 = DataFrameColumn("id", ColumnType.INTEGER, False)
        column_2 = DataFrameColumn(
            "data", ColumnType.NDARRAY, False, [
                2, 2, 3])
        table_info.schema = [column_1, column_2]
        return table_info

    def setUp(self):
        self.table = self.create_sample_table()

    def tearDown(self):
        try:
            shutil.rmtree('dataset', ignore_errors=True)
        except ValueError:
            pass

    def test_should_create_empty_table(self):
        petastorm = PetastormStorageEngine()
        petastorm.create(self.table)
        records = list(petastorm.read(self.table))
        self.assertEqual(records, [])

    def test_should_write_rows_to_table(self):
        dummy_batch = self.create_dummy_batch()

        petastorm = PetastormStorageEngine()
        petastorm.create(self.table)
        petastorm.write(self.table, dummy_batch)

        read_batch = list(petastorm.read(self.table))
        self.assertEqual(len(read_batch), 1)
        self.assertTrue(read_batch, dummy_batch)

    def test_should_return_even_frames(self):
        dummy_batch = self.create_dummy_batch()

        petastorm = PetastormStorageEngine()
        petastorm.create(self.table)
        petastorm.write(self.table, dummy_batch)

        read_batch = list(
            petastorm.read(
                self.table,
                ["id"],
                lambda id: id %
                2 == 0))
        expected_batch = self.create_dummy_batch(
            filters=[
                i for i in range(NUM_FRAMES) if i %
                2 == 0])
        self.assertEqual(len(read_batch), 1)
        self.assertTrue(read_batch, expected_batch)
