import pytest
from lib.Utils import get_spark_session
from lib.DataReader import read_customers,read_orders
from lib.DataManipulation import filter_closed_orders,count_orders_state
from lib.ConfigReader import get_app_config

@pytest.fixture
def spark():
    return get_spark_session("LOCAL")


def test_read_customer_df(spark):
    customers_count=read_customers(spark,"LOCAL").count()
    assert customers_count==12435

def test_read_orders_df(spark):
    orders_count=read_orders(spark,"LOCAL").count()
    assert orders_count==68883

def test_filter_closed_orders(spark):
    orders_df=read_orders(spark,"LOCAL")
    filtered_count=filter_closed_orders(orders_df).count()
    assert filtered_count==7556


def test_read_app_config():
    config=get_app_config("LOCAL")
    assert config["orders.file.path"]=="data/orders.csv"


def test_count_orders_state(spark,expected_results):
    customers_df=read_customers(spark,"LOCAL")
    actual_results=count_orders_state(customers_df)
    assert actual_results.collect() ==expected_results.collect()