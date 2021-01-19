import datetime
import decimal
import uuid
from typing import Any

from hique.base import FieldAttr, NullableFieldAttr


class BigIntField(FieldAttr[int]):
    pass


class NullableBigIntField(NullableFieldAttr[int]):
    pass


class BigSerialField(FieldAttr[int]):
    pass


class NullableBigSerialField(NullableFieldAttr[int]):
    pass


class BooleanField(FieldAttr[bool]):
    pass


class NullableBooleanField(NullableFieldAttr[bool]):
    pass


class CharacterField(FieldAttr[str]):
    pass


class NullableCharacterField(NullableFieldAttr[str]):
    pass


class CharacterVaryingField(FieldAttr[str]):
    pass


class NullableCharacterVaryingField(NullableFieldAttr[str]):
    pass


class DateField(FieldAttr[datetime.date]):
    pass


class NullableDateField(NullableFieldAttr[datetime.date]):
    pass


class DoublePrecisionField(FieldAttr[float]):
    pass


class NullableDoublePrecisionField(NullableFieldAttr[float]):
    pass


class IntegerField(FieldAttr[int]):
    pass


class NullableIntegerField(NullableFieldAttr[int]):
    pass


class IntervalField(FieldAttr[datetime.timedelta]):
    pass


class NullableIntervalField(NullableFieldAttr[datetime.timedelta]):
    pass


class JsonField(FieldAttr[Any]):
    pass


class NullableJsonField(NullableFieldAttr[Any]):
    pass


class JsonbField(FieldAttr[Any]):
    pass


class NullableJsonbField(NullableFieldAttr[Any]):
    pass


class NumericField(FieldAttr[decimal.Decimal]):
    pass


class NullableNumericField(NullableFieldAttr[decimal.Decimal]):
    pass


class RealField(FieldAttr[float]):
    pass


class NullableRealField(NullableFieldAttr[float]):
    pass


class SmallIntField(FieldAttr[int]):
    pass


class NullableSmallIntField(NullableFieldAttr[int]):
    pass


class SmallSerialField(FieldAttr[int]):
    pass


class NullableSmallSerialField(NullableFieldAttr[int]):
    pass


class SerialField(FieldAttr[int]):
    pass


class NullableSerialField(NullableFieldAttr[int]):
    pass


class TextField(FieldAttr[str]):
    pass


class NullableTextField(NullableFieldAttr[str]):
    pass


class TimeField(FieldAttr[datetime.time]):
    pass


class NullableTimeField(NullableFieldAttr[datetime.time]):
    pass


class TimestampField(FieldAttr[datetime.datetime]):
    pass


class NullableTimestampField(NullableFieldAttr[datetime.datetime]):
    pass


class UUIDField(FieldAttr[uuid.UUID]):
    pass


class NullableUUIDField(NullableFieldAttr[uuid.UUID]):
    pass
