from marshmallow import fields

class Utils:
    @classmethod
    def _cast_ddb_value(cls, type_string:str, value):
        if(type_string.upper() == "N"):
            return int(value)
        
        if(type_string.upper() == "L"):
            return_value = []
            for val in value:
                return_value.append(val.get("S"))
            return return_value        
        
        return str(value)
    
    @classmethod
    def _flatten_ddb_list(cls, items):
        return_list = []
        for item in items:
            return_list.append(Utils._flatten_ddb_dict(item))

        return return_list
    
    @classmethod
    def _flatten_ddb_dict(cls, item):
        return_value = {}
        for key in item:
            for key_type in item[key]:
                return_value[key] = Utils._cast_ddb_value(key_type, item[key][key_type])
        return return_value
    
    @classmethod
    def _get_ddb_type(cls, name):        
        return "S"
    
    @classmethod
    def _make_ddb_value(cls, key, value):
        if(type(value) == list):
            return_value = []
            for item in value:
                return_value.append({"S": item})
            return {"L": return_value}
        else:
            return {f"{Utils._get_ddb_type(key)}": value}
    
    @classmethod
    def _marshmallow_to_ddb(self, field):        
        if(type(field) in [fields.Email,fields.Url,fields.UUID]):
            return "S"

        if(type(field) == fields.Number):
            return "N"

        if(type(field) == fields.List):
            return "L"
        
        return "S"