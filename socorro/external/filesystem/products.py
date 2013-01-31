from socorro.external.filesystem.service_base import FileSystemServiceBase

class Products(FileSystemServiceBase):

    def get(self, **kwargs):
        return {
            "products": ["Fennicky"],
            "hits": { "Fennicky": "1.0"},
            "total": 1
        }



