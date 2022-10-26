import json
from decimal import *

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


def toJSON(obj):
     return json.dumps(obj, cls=DecimalEncoder)