from typing import Any


class DataType:
    def simpleString(self) -> str:
        return self.__class__.__name__[:-4].lower()

    def sqlTypeName(self) -> str:
        return self.simpleString()

    def jsonValue(self) -> Any:
        return self.simpleString()

    def __str__(self) -> str:
        return self.simpleString()

    def __repr__(self) -> str:
        return self.__class__.__name__ + "()"

    def __eq__(self, other) -> bool:
        return isinstance(other, self.__class__)

    def __hash__(self) -> int:
        return hash(self.__class__.__name__)

    @classmethod
    def typeName(cls) -> str:
        return cls.__name__

    def _to_iris_sql(self) -> str:
        return self.sqlTypeName()


class IntegerType(DataType):
    def sqlTypeName(self) -> str:
        return "INTEGER"


class LongType(DataType):
    def sqlTypeName(self) -> str:
        return "BIGINT"


class ShortType(DataType):
    def sqlTypeName(self) -> str:
        return "SMALLINT"


class ByteType(DataType):
    def sqlTypeName(self) -> str:
        return "TINYINT"


class FloatType(DataType):
    def sqlTypeName(self) -> str:
        return "FLOAT"


class DoubleType(DataType):
    def sqlTypeName(self) -> str:
        return "DOUBLE"


class DecimalType(DataType):
    def __init__(self, precision: int = 10, scale: int = 0) -> None:
        self.precision = precision
        self.scale = scale

    def simpleString(self) -> str:
        return f"decimal({self.precision},{self.scale})"

    def sqlTypeName(self) -> str:
        return f"NUMERIC({self.precision},{self.scale})"

    def jsonValue(self) -> str:
        return f"decimal({self.precision},{self.scale})"

    def __eq__(self, other) -> bool:
        return (isinstance(other, DecimalType)
                and self.precision == other.precision
                and self.scale == other.scale)

    def __hash__(self) -> int:
        return hash((self.__class__.__name__, self.precision, self.scale))


class StringType(DataType):
    def sqlTypeName(self) -> str:
        return "VARCHAR(4000)"

    def simpleString(self) -> str:
        return "string"


class BooleanType(DataType):
    def sqlTypeName(self) -> str:
        return "BIT"

    def simpleString(self) -> str:
        return "boolean"


class DateType(DataType):
    def sqlTypeName(self) -> str:
        return "DATE"


class TimestampType(DataType):
    def sqlTypeName(self) -> str:
        return "TIMESTAMP"


class BinaryType(DataType):
    def sqlTypeName(self) -> str:
        return "BINARY"


class NullType(DataType):
    def sqlTypeName(self) -> str:
        return "VARCHAR(1)"

    def simpleString(self) -> str:
        return "null"


class CharType(DataType):
    def __init__(self, length: int = 1) -> None:
        self.length = length

    def simpleString(self) -> str:
        return f"char({self.length})"

    def sqlTypeName(self) -> str:
        return f"CHAR({self.length})"

    def jsonValue(self) -> str:
        return f"char({self.length})"


class VarcharType(DataType):
    def __init__(self, length: int = 255) -> None:
        self.length = length

    def simpleString(self) -> str:
        return f"varchar({self.length})"

    def sqlTypeName(self) -> str:
        return f"VARCHAR({self.length})"

    def jsonValue(self) -> str:
        return f"varchar({self.length})"


class TimestampNTZType(DataType):
    def sqlTypeName(self) -> str:
        return "TIMESTAMP"

    def simpleString(self) -> str:
        return "timestamp_ntz"


class ArrayType(DataType):
    def __init__(self, elementType: DataType, containsNull: bool = True) -> None:
        self.elementType = elementType
        self.containsNull = containsNull

    def simpleString(self) -> str:
        return f"array<{self.elementType}>"

    def sqlTypeName(self) -> str:
        return "VARCHAR(255)"

    def jsonValue(self) -> dict:
        return {"type": "array", "elementType": self.elementType.jsonValue(), "containsNull": self.containsNull}


class MapType(DataType):
    def __init__(self, keyType: DataType, valueType: DataType, valueContainsNull: bool = True) -> None:
        self.keyType = keyType
        self.valueType = valueType
        self.valueContainsNull = valueContainsNull

    def simpleString(self) -> str:
        return f"map<{self.keyType},{self.valueType}>"

    def sqlTypeName(self) -> str:
        return "VARCHAR(255)"

    def jsonValue(self) -> dict:
        return {"type": "map", "keyType": self.keyType.jsonValue(), "valueType": self.valueType.jsonValue(), "valueContainsNull": self.valueContainsNull}


class StructField:
    def __init__(self, name: str, dataType: DataType, nullable: bool = True, metadata: Any = None) -> None:
        self.name = name
        self.dataType = dataType
        self.nullable = nullable
        self.metadata = metadata

    def __repr__(self) -> str:
        return f"StructField({self.name!r},{self.dataType!r},{self.nullable!r})"


class StructType(DataType):
    def __init__(self, fields: list[StructField] | None = None) -> None:
        self.fields = fields or []

    def simpleString(self) -> str:
        inner = ", ".join(f"{f.name}:{f.dataType}" for f in self.fields)
        return f"struct<{inner}>"

    def sqlTypeName(self) -> str:
        return "VARCHAR(255)"

    def jsonValue(self) -> dict:
        return {"type": "struct", "fields": [{"name": f.name, "type": f.dataType.jsonValue(), "nullable": f.nullable, "metadata": f.metadata} for f in self.fields]}

    def add(self, field, *args, **kwargs):
        if isinstance(field, StructField):
            self.fields.append(field)
        elif isinstance(field, str):
            self.fields.append(StructField(field, *args, **kwargs))
        return self

    def __iter__(self):
        return iter(self.fields)
