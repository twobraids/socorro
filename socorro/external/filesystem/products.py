from socorro.external.filesystem.service_base import FileSystemServiceBase

class Products(FileSystemServiceBase):

    def get(self, **kwargs):
        return {
            "products": [
                "Fennecky",
                "WaterWolf",
                "Caminimal"
            ],
            "hits": {
                "Fennecky": [
                    {
                        "product": "Fennecky",
                        "version": "42",
                        "start_date": "2001-01-01",
                        "end_date": "2099-01-01",
                        "throttle": 10.0,
                        "featured": false,
                        "release": "Nightly",
                        "has_builds": true
                    },
                ],
                "Thunderbird": [
                    {}
                ],
                "Caminimal": [
                    {}
                ]
            },
            "total": 6
        }



