# Relationships
## Definitions
- Parent table: The table that the current schema points to.
- Child table: The current schema.
- RCU: Read Capacity Units
- WCU: Write Capacity Units

## Technical
Relationships are configured using the Marshmallow ```Nested``` field, where the first argument is the schema of the parent table. By default, the relationship will be joined on the ```hash_key``` defined in the parent schema. The ```fk``` argument is required for all relationships and it indicates the field in the child schema that is to be joined on.

Under the hood, the pk and fk arguments are added to the ```metadata``` argument within Marshmallow's ```Field``` object.

## Limitations
- Relationships are not recursive and are only evaluated to a depth of 1.

## Performance Considerations
- Relationships are inefficient by design and increase the computational cost of every operation.
- Under the hood, relationships use ```scan``` to fetch data, this will result in additional RCU costs.

## Roadmap
- Improve effeciency by utilizing ```query``` when joining data on a ```hash_key```.

## Simple Relationship
```
client = fields.Nested(Client, fk="client_id")
```

## Advanced Relationship
If the relationship needs to be joined on a field that is not the parent table's ```hash_id```, the ```pk``` argument must be passed and should be the name of the field inside the parent schema that the data is to be joined on.

```
client = fields.Nested(Client, fk="client_id", pk="id")
```

## Examples
```
class Client(Model):
    class Meta():
        table_name = client_table_name
        hash_key = "id"
        range_key = None

    class Schema():
        id = fields.String(default=str(uuid4()))
        name = fields.String()
```

```
class Contact(Model):
    class Meta():
        table_name = contact_table_name
        hash_key = "id"
        range_key = None
        read = 25
        write = 25

    class Schema():
        id = fields.String(required=True, default=str(uuid4()))
        firstname = fields.String(required=True)
        lastname = fields.String(required=True)
        email = fields.String(required=True)
        phone = fields.String(required=True)
        client_id = fields.String(required=True)
        client = fields.Nested(Client, fk="client_id")
```