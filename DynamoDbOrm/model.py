import boto3, json, random, time
from marshmallow import fields
from .relationships import Relationship
from concurrent.futures import ThreadPoolExecutor, as_completed
from .utils import Utils

class OrmResponse():
    def __init__(self, cls, data:any):
        for column in cls.columns:
            setattr(self, column, None)

        for key in data:
            setattr(self, key, data[key])
    
    def __repr__(self):
        return json.dumps(self.__dict__)

class Model():
    table_name = None
    key_id = None

    def __init__(self) -> None:
        self._client = boto3.client("dynamodb")
        self._relationships = []
        self._relationships_processed = False
        self._columns = None
        self._ref_list = {}
        self._refs = {}

    @property
    def columns(self):
        # object must be initialized, ex: Contact()
        if(self._columns):
            return self._columns # don't reprocess, returned from cache
        
        self._columns = {}
        for item in dict(self.Schema._declared_fields):
            if(not item.startswith("__")):
                self._columns[item] = self.Schema._declared_fields[item]
        return self._columns

    @property
    def relationships(self):
        if(self._relationships_processed):
            return self._relationships # don't reprocess, returned from cache
        
        self._relationships_processed = True
        for key in self.columns:
            column:fields.Field = self.columns[key]
            if(type(column) == fields.Nested):
                if("fk" in column.metadata):
                    fk = column.metadata.get("fk")
                else:
                    fk = self.Meta.hash_key

                if("pk" in column.metadata):
                    pk = column.metadata.get("pk")
                else:
                    pk = column.nested.Meta.hash_key

                self._relationships.append(Relationship(column.nested.Meta.table_name, self.Meta.table_name, pk, fk, key, column))
        return self._relationships
    
    def _gather_refs(self, records, relationship:Relationship):
        """
        loop over all the returned records and store the foreign key in a list. this list will be accessed by _fetch_refs
        where the boto3's batch_get_item is used to get all the matching records.

        _ref_list = {
            "parent_table1": {"Keys":[...]},
            "parent_table2": {"Keys":[...]}
        }
        """
        self._ref_list[relationship.parent_table] = {"Keys": []}
        id_list = []
        for record in records:
            key = record.get(relationship.child_key)
            if(key):
                if(relationship.column.many == True):
                    for i in key:
                        if(i not in id_list):
                            id_list.append(i)
                            self._ref_list[relationship.parent_table]["Keys"].append({relationship.parent_key:{"S": i}})
                else:
                    if(key not in id_list):
                        id_list.append(key)
                        self._ref_list[relationship.parent_table]["Keys"].append({relationship.parent_key:{"S": key}})
        self._fetch_refs()

    def _batch_get_items(self, table_name, keys):
        """
        Fetch a batch of items from a DynamoDB table.
        :param table_name: Name of the table.
        :param keys: List of keys for this batch.
        :return: Retrieved items.
        """
        retrieved_items = []
        unprocessed_keys = {'Keys': keys}
        retries = 0

        while unprocessed_keys.get('Keys'):
            try:
                response = self._client.batch_get_item(
                    RequestItems={
                        table_name: unprocessed_keys
                    }
                )
                retrieved_items.extend(response['Responses'].get(table_name, []))
                unprocessed_keys = response.get('UnprocessedKeys', {}).get(table_name, {})
                
                if unprocessed_keys:
                    retries += 1
                    wait_time = min(2 ** retries + random.uniform(0, 1), 30)
                    print(f"[Retry] Retrying {len(unprocessed_keys['Keys'])} keys after {wait_time:.2f} seconds...")
                    time.sleep(wait_time)
            except Exception as e:
                print(f"Error in batch_get_item: {e}")
                break
        
        return retrieved_items
    
    def _fetch_refs(self):
        """
        this function pulls data from the parent table, as defined in the child table's schema, and stores it in
        the _refs variable

        _refs = {
            "parent_table1" : [{...}],
            "parent_table2" : [{...}]
        }
        """
        batch_size = 100 # batch_get_item only returns 100 rows, so large queries must be broken into batches
        max_threads = 10 # max threads TODO: base this value on the number of available CPUs
        responses = {}
        for table in self._ref_list:
            responses[table] = []
            keys = self._ref_list[table]["Keys"]
            key_batches = [keys[i:i + batch_size] for i in range(0, len(keys), batch_size)] # split the list into batches of 100

            with ThreadPoolExecutor(max_threads) as executor:
                # fetch data in parallel
                futures = [executor.submit(self._batch_get_items, table, batch) for batch in key_batches]

                for future in as_completed(futures):
                    try:
                        result = future.result()
                        responses[table].extend(result)
                    except Exception as e:
                        print(f"Error processing a batch: {e}")

        if(responses):
            for table_name in responses:
                self._refs[table_name] = {}
                records = Utils._flatten_ddb_list(responses.get(table_name))
                self._refs[table_name] = records
    
    def _join_tables(self, source_table, lookup_table, relationship:Relationship):
        lookup_dict = {}
        for lookup_item in lookup_table:
            key = lookup_item.get(relationship.parent_key)
            if(key):
                lookup_dict[key] = lookup_item

        for record in source_table:
            key = record.get(relationship.child_key)
            if(key):
                if(type(key) == list):
                    record[relationship.field_name] = []
                    for v in key:
                        record[relationship.field_name].append(lookup_dict[v])
                else:
                    if(key in lookup_dict):
                        record[relationship.field_name] = lookup_dict[key]
    
    @classmethod
    def table_exists(cls) -> bool:
        _self = cls()
        try:
            _self._client.describe_table(TableName=_self.Meta.table_name)
            return True
        except:
            return False

    @classmethod
    def make_schema(cls):
        try:
            _self = cls()
            args = {
                "TableName": _self.Meta.table_name, 
                "KeySchema": [{"AttributeName": _self.Meta.hash_key,"KeyType": "HASH"}],
                "AttributeDefinitions": [],
                "BillingMode": "PAY_PER_REQUEST"
            }

            if(_self.Meta.sort_key):
                args["KeySchema"].append({"AttributeName": _self.Meta.sort_key,"KeyType": "RANGE"})
            
            read = None
            write = None
            if(_self.Meta.__dict__.get("read")):
                read = _self.Meta.read
                write = 5

            if(_self.Meta.__dict__.get("write")):
                write = _self.Meta.write

            if(read and write):
                args["ProvisionedThroughput"] = {"ReadCapacityUnits": read, "WriteCapacityUnits": write}
                args["BillingMode"] = "PROVISIONED"

            hash_key = _self.Schema._declared_fields[_self.Meta.hash_key]
            hash_key_type = Utils._marshmallow_to_ddb(hash_key)
            args["AttributeDefinitions"].append({"AttributeName": _self.Meta.hash_key, "AttributeType": hash_key_type})

            if(_self.Meta.sort_key):
                sort_key = _self.Schema._declared_fields[_self.Meta.sort_key]
                sort_key_type = Utils._marshmallow_to_ddb(sort_key)
                args["AttributeDefinitions"].append({"AttributeName": _self.Meta.hash_key, "AttributeType": sort_key_type})

            _self._client.create_table(**args)
            waiter = _self._client.get_waiter("table_exists")
            waiter.wait(TableName=_self.Meta.table_name)
            return True
        except Exception as e:
            print(str(e))
            return False
    
    @classmethod
    def create(cls, **kwargs):
        _self = cls()

        for key in _self.columns:
            column:fields.Field = _self.columns[key]
            if(key not in kwargs):
                if(column.default):
                    kwargs[key] = column.default

        item = {}
        for key in kwargs:
            item[key] = Utils._make_ddb_value(key, kwargs[key])
        _self._client.put_item(TableName=_self.Meta.table_name, Item=item, ReturnValues="ALL_OLD", ConditionExpression=f"attribute_not_exists({_self.Meta.hash_key})")
        return kwargs

    @classmethod
    def update(cls, **kwargs):
        _self = cls()

        id = kwargs[_self.Meta.hash_key]
        set_expression = []
        remove_expression = []
        expression_attribute_names = {}
        expression_attribute_values = {}

        for key, value in kwargs.items():
            if(key != _self.Meta.hash_key):
                placeholder_name = f"#{key}"
                if value is None:
                    remove_expression.append(placeholder_name)
                else:
                    set_expression.append(f"{placeholder_name} = :{key}")
                    attr_type = Utils._get_ddb_type(key)
                    if(type(value) == list):
                        expression_attribute_values[f":{key}"] = Utils._make_ddb_value(key, value)
                    else:
                        expression_attribute_values[f":{key}"] = {attr_type: value}
                expression_attribute_names[placeholder_name] = key

        update_expression = ""
        if set_expression:
            update_expression += "SET " + ", ".join(set_expression)

        if remove_expression:
            update_expression += " REMOVE " + ", ".join(remove_expression)

        args = {
            "TableName": _self.Meta.table_name,
            "Key" : {_self.Meta.hash_key: Utils._make_ddb_value(_self.Meta.hash_key, id)},
            "UpdateExpression" : update_expression,
            "ExpressionAttributeNames" : expression_attribute_names,
            "ConditionExpression" : f"attribute_exists({_self.Meta.hash_key})",
            "ReturnValues" : "ALL_NEW"
        }

        if(expression_attribute_values):
            args["ExpressionAttributeValues"] = expression_attribute_values

        res = _self._client.update_item(**args)

    @classmethod
    def delete(cls, id):
        _self = cls()
        item = {_self.Meta.hash_key: Utils._make_ddb_value(_self.Meta.hash_key, id)}
        _self._client.delete_item(TableName=_self.Meta.table_name, Key=item)

    @classmethod
    def get(cls, partition_key, sort_key=None):
        _self = cls()
        
        keys = {_self.Meta.hash_key: Utils._make_ddb_value(_self.Meta.hash_key, partition_key)}
        if(sort_key):
            keys[_self.Meta.sort_key] = Utils._make_ddb_value(_self.Meta.sort_key, sort_key)

        res = _self._client.get_item(TableName=_self.Meta.table_name, Key=keys)
        keys = res.get("Item", {})
        data = Utils._flatten_ddb_dict(keys)
        records = []
        records.append(data)

        for relationship in _self.relationships:
            _self._gather_refs(records, relationship)
            _self._join_tables(records, _self._refs[relationship.parent_table], relationship)

        return OrmResponse(_self, records[0])
    
    @classmethod
    def scan(cls, **kwargs):
        _self = cls()
        args = {
            "TableName": _self.Meta.table_name
        }

        limit = kwargs.get("limit")
        if(limit):
            args["Limit"] = limit

        last_evaluated_key = kwargs.get("last_evaluated_key")
        if(last_evaluated_key):
            args["ExclusiveStartKey"] = last_evaluated_key

        filter_list = []
        attribute_names = {}
        attribute_values = {}
        filter_list = []
        for key in kwargs:
            if(key in _self.columns):
                value = kwargs[key]
                filter_list.append(f"#{key} = :{key}")
                attribute_names[f"#{key}"] = key
                attribute_values[f":{key}"] = Utils._make_ddb_value(key, value)

        if(len(filter_list)):
            args["FilterExpression"] = " And ".join(filter_list)
            args["ExpressionAttributeNames"] = attribute_names
            args["ExpressionAttributeValues"] = attribute_values

        if("Limit" not in args):
            items = []
            paginator = _self._client.get_paginator("scan")
            for page in paginator.paginate(**args):
                items.extend(page.get("Items", []))
        else:
            res = _self._client.scan(**args)
            items = res.get("Items", [])
            if("LastEvaluatedKey" in res):
                cls.last_evaluated_key = res.get("LastEvaluatedKey")

        records = Utils._flatten_ddb_list(items)
        if(len(items)):
            for relationship in _self.relationships:
                _self._gather_refs(records, relationship)
                _self._join_tables(records, _self._refs[relationship.parent_table], relationship)        

        return records
