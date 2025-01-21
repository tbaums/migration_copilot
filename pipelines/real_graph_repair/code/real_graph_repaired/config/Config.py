from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, ORDER_FILE: str=None, OP_AGG_RPT_FILE: str=None, CUSTOMER_FILE: str=None, **kwargs):
        self.spark = None
        self.update(ORDER_FILE, OP_AGG_RPT_FILE, CUSTOMER_FILE)

    def update(
            self,
            ORDER_FILE: str="/adshome/apatn19/orders.csv",
            OP_AGG_RPT_FILE: str="/tmp/cust_agg_rpt.csv",
            CUSTOMER_FILE: str="/adshome/apatn19/data.csv",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.ORDER_FILE = ORDER_FILE
        self.OP_AGG_RPT_FILE = OP_AGG_RPT_FILE
        self.CUSTOMER_FILE = CUSTOMER_FILE
        pass
