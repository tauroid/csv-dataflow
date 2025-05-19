from dataclasses import dataclass


@dataclass(frozen=True)
class NetCDF:
    standard_name: str
    grid_mapping_name: str
    cell_methods: str

@dataclass(frozen=True)
class GRIB:
    @dataclass(frozen=True)
    class Section3:
        x: str

    class Template3_0(Section3): ...
    class Template3_140(Section3): ...

    @dataclass(frozen=True)
    class Section4:
        x: str
        discipline: int
        category: int
        number: int

    class Template4_0(Section4): ...
    class Template4_8(Section4): ...

    section_3: Template3_0 | Template3_140
    section_4: Template4_0 | Template4_8

