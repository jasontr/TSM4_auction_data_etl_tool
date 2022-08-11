from collections import namedtuple
import csv
from enum import Enum
import os
import re
from typing import Dict, Iterable, List, Optional, Tuple, Union
import io
from datetime import datetime
from slpp import slpp as lua


# Type definitions
Fraction = Server = CSVRaw = ItemId = ItemName = TimestampInfo = str

def escape_slash(raw_str):
    return raw_str.encode().decode('unicode_escape')


def csv_dict_reader(csv_text: CSVRaw) -> csv.DictReader:
    with io.StringIO(csv_text) as csv_raw:
        return csv.DictReader(csv_raw)


def convert_unixtime(unixtime: Union[str, int], format='%Y-%m-%d %H:%M:%S'):
    record_datetime = datetime.fromtimestamp(int(unixtime))
    return record_datetime.strftime(format)


class Extract:
    def __init__(self, source_file):
        self.source_file = source_file

    def extract(self) -> Tuple[Fraction, Server, TimestampInfo, CSVRaw, Dict[ItemId, ItemName]]:
        trade_skill_master_db, tsm_item_info_db = self.find_raw_data(self.source_file)
        (fraction, server, auction_raw_data) = self.ingest_auction_data(trade_skill_master_db)
        item_index = self.ingest_item_info_data(tsm_item_info_db)
        timestamp_info_for_filename = self.get_time_info(auction_raw_data)
        return fraction, server, timestamp_info_for_filename, auction_raw_data, item_index

    @staticmethod
    def find_raw_data(path) -> Tuple[dict, dict]:
        trade_skill_master_db_raw = ""
        tsm_item_info_db_raw = ""
        with open(path, 'r', encoding='UTF-8') as f:
            for line in f:
                if 'TSMItemInfoDB' in line:
                    tsm_item_info_db_raw += line.replace('TSMItemInfoDB = ', '')
                    break
                trade_skill_master_db_raw += line.replace('TradeSkillMasterDB = ', '')
            for line in f:
                tsm_item_info_db_raw += line
        return lua.decode(trade_skill_master_db_raw), lua.decode(tsm_item_info_db_raw)


    @staticmethod
    def ingest_auction_data(trade_skill_master_db: dict) -> Tuple[Fraction, Server, CSVRaw]:
        csv_data_key_pattern = re.compile(r'f@(.+) - (.+)@internalData@csvAuctionDBScan')

        for key, value in trade_skill_master_db.items():
            match_obj = csv_data_key_pattern.search(key)
            if match_obj:
                fraction, server = match_obj.groups()
                return (fraction, server, escape_slash(value))

    @staticmethod
    def item_id_regularization(item_id: ItemId) -> ItemId:
        separator = ":"
        valid_composition_number = 2
        compositions = item_id.split(separator)
        regularized_item_id = separator.join(compositions[:valid_composition_number])
        return regularized_item_id

    @staticmethod
    def create_item_index(tsm_item_info_db: dict) -> Dict[ItemId, ItemName]:
        item_id_list = tsm_item_info_db.get('itemStrings')
        item_name_list = tsm_item_info_db.get('names')
        def create_item_index_tuple():
            separator = '\x02'
            for item_ids, item_names in zip(item_id_list, item_name_list):
                for item_id, item_name in zip(item_ids.split(separator), item_names.split(separator)):
                    item_id = Extract.item_id_regularization(item_id)
                    yield item_id, item_name
        return dict(create_item_index_tuple())

    @staticmethod
    def ingest_item_info_data(tsm_item_info_db: dict) -> Dict[ItemId, ItemName]:
        return Extract.create_item_index(tsm_item_info_db)

    @staticmethod
    def get_time_info(auction_raw_data):
        with io.StringIO(auction_raw_data) as csv_raw:
            sample_row = next(csv.DictReader(csv_raw))
        timestamp_format_for_filename = '%Y%m%d-%H%M%S'
        return convert_unixtime(sample_row.get('lastScan'), timestamp_format_for_filename)


ColumnDefinition = namedtuple('ColumnDefinition', ['before_transform', 'raw', 'transformed'])


class OriginalColumns(Enum):
    ItemId = ColumnDefinition(before_transform='itemString', raw='itemString', transformed='itemName')
    MinBuyout = ColumnDefinition(before_transform='minBuyout', raw='minBuyout', transformed='minBuyoutFormatted')
    MarketValue = ColumnDefinition(before_transform='marketValue', raw='marketValue', transformed='marketValueFormatted')
    LastScan = ColumnDefinition(before_transform='lastScan', raw='lastScanUnixtime', transformed='lastScan')
    Quantity = ColumnDefinition(before_transform='quantity', raw='quantity', transformed=None)
    NumAuctions = ColumnDefinition(before_transform='numAuctions', raw='numAuctions', transformed=None)

    @property
    def raw(self):
        return self.value.raw

    @property
    def transformed(self):
        return self.value.transformed

    @property
    def before_transform(self):
        return self.value.before_transform
    

class Transform:
    @staticmethod
    def convert2gold(currency_in_copper: Union[str, int]) -> str:
        currency_in_copper = str(currency_in_copper)
        remains, copper = currency_in_copper[:-2], currency_in_copper[-2:]
        gold, silver = remains[:-2], remains[-2:]
        formatted_currency = ''
        if gold:
            formatted_currency += f'{gold}G '
        if silver:
            formatted_currency += f'{silver}S '
        if copper:
            formatted_currency += f'{copper}C'
        return formatted_currency

    @staticmethod
    def transform(auction_raw_data: CSVRaw, item_index: Dict[ItemId, ItemName]) -> Iterable:
        with io.StringIO(auction_raw_data) as csv_raw:
            for row in csv.DictReader(csv_raw):
                row[OriginalColumns.ItemId.transformed] = item_index.get(row.get(OriginalColumns.ItemId.before_transform))
                row[OriginalColumns.MinBuyout.transformed] = Transform.convert2gold(row.get(OriginalColumns.MinBuyout.before_transform))
                row[OriginalColumns.MarketValue.transformed] = Transform.convert2gold(row.get(OriginalColumns.MarketValue.before_transform))
                row[OriginalColumns.LastScan.raw] = row.get(OriginalColumns.LastScan.before_transform)
                row[OriginalColumns.LastScan.transformed] = convert_unixtime(row.get(OriginalColumns.LastScan.raw))
                yield row


class Load:
    COLUMNS_ORDER = [
        OriginalColumns.ItemId.raw, OriginalColumns.MinBuyout.raw, OriginalColumns.MarketValue.raw, OriginalColumns.LastScan.raw,
        OriginalColumns.ItemId.transformed, OriginalColumns.MinBuyout.transformed, OriginalColumns.MarketValue.transformed, OriginalColumns.LastScan.transformed,
        OriginalColumns.NumAuctions.raw, OriginalColumns.Quantity.raw
    ]

    def __init__(self, path, header=None):
        self.header = header or self.COLUMNS_ORDER
        self.path = path

    def load(self, rows: Iterable, mode='w', path=None, header: Optional[Union[Tuple, List]]=None):
        header = header or self.header
        path = path or self.path
        with open(path, mode) as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            for row in rows:
                writer.writerow({key: row.get(key) for key in header})


def etl(source_path, artifacts_path, artifacts_name_template='{server}_{fraction}_{timestamp}.csv'):
    extract = Extract(source_path)
    fraction, server, timestamp, auction_raw_data, item_index = extract.extract()
    
    transformed = Transform.transform(auction_raw_data, item_index)

    artifact_name = artifacts_name_template.format(server=server, fraction=fraction, timestamp=timestamp)
    target_path = os.path.join(artifacts_path, artifact_name)
    load = Load(target_path)
    load.load(transformed)
