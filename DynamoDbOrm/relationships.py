class Relationship():
    def __init__(self, parent_table, child_table, parent_key, child_key, field_name, column) -> None:
        self.parent_table = parent_table
        self.child_table = child_table
        self.parent_key = parent_key
        self.child_key = child_key
        self.field_name = field_name
        self.column = column