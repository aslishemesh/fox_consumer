from fox_consumer import FoxItem
import unittest
import json

class TestFoxItem(unittest.TestCase):
    def setUp(self):
        self.fox_item = FoxItem("test_img", "test_main_cat", "test_type", "test_name", 10)

    def test_verify_invalid_json(self):
        self.assertEquals(FoxItem.verify_json(json.dumps("data")), False)

    def test_verify_valid_json(self):
        json_data = self.fox_item.to_json()
        self.assertEquals(FoxItem.verify_json(json.dumps(json_data)), True)

    def test_from_json(self):
        json_data = self.fox_item.to_json()
        json_item = self.fox_item.from_json(json.dumps(json_data))
        self.assertEquals(json_item, json_item)

if __name__ == "__main__":
    unittest.main()
