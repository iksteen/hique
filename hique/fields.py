from typing import Any, Optional

from hique.base import FieldAttr, FieldImplBase, NullableFieldAttr


class TextFieldImpl(FieldImplBase[str]):
    @staticmethod
    def from_python(value: Any) -> Optional[str]:
        if not isinstance(value, str):
            raise TypeError(f"Expected string, got {type(value).__name__}.")
        return value

    @staticmethod
    def to_python(value: Optional[str]) -> Optional[str]:
        return value


class TextField(FieldAttr[str, TextFieldImpl]):
    impl_type = TextFieldImpl


class NullableTextField(NullableFieldAttr[str, TextFieldImpl], TextField):
    pass
