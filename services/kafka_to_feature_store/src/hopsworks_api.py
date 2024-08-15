import hopsworks
import pandas as pd

from src.config import config

class HopsworksApi:
    def __init__(self, feature_group_name: str, feature_group_version: int):
        self.feature_group_name = feature_group_name
        self.feature_group_version = feature_group_version

        # Authenticate with the Hopsworks API
        project = hopsworks.login(
            project=config.hopsworks_project_name, 
            api_key_value=config.hopsworks_api_key
        )

        # Gets the feature store
        feature_store = project.get_feature_store()

        # Get or create the feature group
        self.ohlc_feature_group = feature_store.get_or_create_feature_group(
            name=feature_group_name,
            version=feature_group_version,
            description='OHLC data coming from Kraken API',
            primary_key=['product_id', 'timestamp'],
            event_time='timestamp',
            online_enabled=True,
        )

    def push_data_to_feature_store(self, data: list) -> None:
        """
        Pushes the given `data`
        """   

        # transform the data to a Pandas dataset
        data = pd.DataFrame(data)

        # write to the feature group
        self.ohlc_feature_group.insert(data)
