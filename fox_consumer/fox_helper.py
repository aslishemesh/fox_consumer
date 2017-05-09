import jsonschema
import json


class FoxItem(object):
    """
    The FoxItem class
    """
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "title": "FoxItem",
        "description": "An item for Fox catalog",
        "type": "object",
        "properties": {
            "item_img_id": {"type": "string"},
            "item_main_category": {"type": "string"},
            "item_type": {"type": "string"},
            "item_name": {"type": "string"},
            "item_price": {"type": "number"},
        },
        "required": ["item_img_id", "item_main_category", "item_type", "item_name", "item_price"]
    }

    def __init__(self, item_img_id, item_main_category, item_type, item_name, item_price):
        self.item_img_id = item_img_id
        self.item_main_category = item_main_category
        self.item_type = item_type
        self.item_name = item_name
        self.item_price = item_price

    def to_json(self):
        """
        convert the FoxItem into json supported type
        :return: a dictionary to support json dumps
        """
        return {'item_img_id': self.item_img_id,
                'item_main_category': self.item_main_category,
                'item_type': self.item_type,
                'item_name': self.item_name,
                'item_price': self.item_price
                }

    def __eq__(self, other):
        """Override the default Equals behavior to support assert-equal"""
        if isinstance(other, self.__class__):
            return (self.item_img_id == other.item_img_id
                    and self.item_price == other.item_price
                    and self.item_name == other.item_name
                    and self.item_main_category == other.item_main_category
                    and self.item_type == other.item_type)
        return TypeError

    @staticmethod
    def verify_json(data):
        """
        Verify json input according to the schema defined for FoxItem class
        :param data: json input from rabbitmq server
        :param schema: json schema for FoxItem class 
        :return: True/False (verified/not)
        """
        try:
            json_data = json.loads(data)
            jsonschema.validate(json_data, FoxItem.schema)
            return True
        except jsonschema.ValidationError as e:
            print e
        except jsonschema.SchemaError as e:
            print e
        except ValueError as e:
            print e
        return False

    @staticmethod
    def from_json(json_obj):
        """
        This function will convert the json to FoxItem class
        :param obj: json object
        :return: FoxItem object
        """
        if FoxItem.verify_json(json_obj):
            json_item = json.loads(json_obj)
            item = FoxItem(**json_item)
            return item
        else:
            raise Exception("Could not convert from json")
