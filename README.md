# TSM4_auction_data_etl_tool
## What's this
A ETL pipeline for auction data generated by [TradeSkillMaster](https://www.tradeskillmaster.com/)4.

## How to use
```python
from tsm4_data_etl import etl


etl(
    source_path='<path to wow client>\_classic_\WTF\Account\<user account>\SavedVariables\TradeSkillMaster.lua',
    artifacts_path='<path to save csv data>'
)
```