from hique.base import FieldAttr, NullableFieldAttr


class TextField(FieldAttr[str]):
    pass


class NullableTextField(NullableFieldAttr[str], TextField):
    pass
