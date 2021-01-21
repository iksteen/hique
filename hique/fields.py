import datetime
import decimal
import ipaddress
import uuid
from typing import Any, Union

from hique.base import Field, NullableField


class BigIntField(Field[int]):
    pass


class NullableBigIntField(NullableField[int]):
    pass


class BigSerialField(Field[int]):
    pass


class NullableBigSerialField(NullableField[int]):
    pass


class BooleanField(Field[bool]):
    pass


class NullableBooleanField(NullableField[bool]):
    pass


class ByteaField(Field[bytes]):
    pass


class NullableByteaField(Field[bytes]):
    pass


class CharacterField(Field[str]):
    pass


class NullableCharacterField(NullableField[str]):
    pass


class CharacterVaryingField(Field[str]):
    pass


class NullableCharacterVaryingField(NullableField[str]):
    pass


class CidrField(Field[Union[ipaddress.IPv4Network, ipaddress.IPv6Network]]):
    pass


class NullableCidrField(
    NullableField[Union[ipaddress.IPv4Network, ipaddress.IPv6Network]]
):
    pass


class DateField(Field[datetime.date]):
    pass


class NullableDateField(NullableField[datetime.date]):
    pass


class DoublePrecisionField(Field[float]):
    pass


class NullableDoublePrecisionField(NullableField[float]):
    pass


class InetField(Field[Union[ipaddress.IPv4Address, ipaddress.IPv6Address]]):
    pass


class NullableInetField(
    NullableField[Union[ipaddress.IPv4Address, ipaddress.IPv6Address]]
):
    pass


class IntegerField(Field[int]):
    pass


class NullableIntegerField(NullableField[int]):
    pass


class IntervalField(Field[datetime.timedelta]):
    pass


class NullableIntervalField(NullableField[datetime.timedelta]):
    pass


class JsonField(Field[Any]):
    pass


class NullableJsonField(NullableField[Any]):
    pass


class JsonbField(Field[Any]):
    pass


class NullableJsonbField(NullableField[Any]):
    pass


class NumericField(Field[decimal.Decimal]):
    pass


class NullableNumericField(NullableField[decimal.Decimal]):
    pass


class RealField(Field[float]):
    pass


class NullableRealField(NullableField[float]):
    pass


class SmallIntField(Field[int]):
    pass


class NullableSmallIntField(NullableField[int]):
    pass


class SmallSerialField(Field[int]):
    pass


class NullableSmallSerialField(NullableField[int]):
    pass


class SerialField(Field[int]):
    pass


class NullableSerialField(NullableField[int]):
    pass


class TextField(Field[str]):
    pass


class NullableTextField(NullableField[str]):
    pass


class TimeField(Field[datetime.time]):
    pass


class NullableTimeField(NullableField[datetime.time]):
    pass


class TimestampField(Field[datetime.datetime]):
    pass


class NullableTimestampField(NullableField[datetime.datetime]):
    pass


class UUIDField(Field[uuid.UUID]):
    pass


class NullableUUIDField(NullableField[uuid.UUID]):
    pass
