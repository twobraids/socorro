class Products(object):

    def get(self, **kwargs):
        return {
            "products": ["Fennicky"],
            "hits": { "Fennicky": "1.0"},
            "total": 1
        }



