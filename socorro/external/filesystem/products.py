class Products(object):

    def get(self, **kwargs):
        print kwargs
        return {
            "products": ["Fennicky"],
            "hits": { "Fennicky": "1.0"},
            "total": 1
        }



