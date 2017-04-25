
class FoxItem(object):
    """
    The FoxItem class
    """
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
        return {'item_main_category': self.item_main_category,
                'item_type': self.item_type,
                'item_img_id': self.item_img_id,
                'item_price': self.item_price,
                'item_name': self.item_name
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
