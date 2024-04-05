from rainforest.etl.bronze.buyer import BuyerBronzeETL

class TestBuyerBronzeETL:

    def test_extract_upstream(self, spark):
        buyer_tbl = BuyerBronzeETL(spark=spark)
        buyer_etl_dataset = buyer_tbl.extract_upstream()
        assert buyer_etl_dataset[0].name == "buyer"
    
    def test_transform_upstream(self, spark):
        pass