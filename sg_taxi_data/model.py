"""Model classes for the Singapore Taxi Data API"""

from typing import Any

from pydantic import BaseModel, Field, computed_field, field_validator


class Coordinate(BaseModel, extra="ignore"):
    """GeoJSON coordinate object"""

    coordinate: list[float] = Field(description="Longtitude and Latitude position", frozen=True)

    @field_validator("coordinate")
    @classmethod
    def validate_coordinate(cls, v):
        """Validate the coordinate field to ensure it is a list of 2 element"""

        if isinstance(v, list) and len(v) != 2:
            raise ValueError("Coordinate must have 2 elements")
        return v

    @computed_field
    def longtitude(self) -> float:
        """Return the longtitude of the coordinate"""

        return tuple(self.coordinate)[0]

    @computed_field
    def latitude(self) -> float:
        """Return the latitude of the coordinate"""

        return tuple(self.coordinate)[1]


class Geometry(BaseModel, extra="ignore"):
    """List of GeoJSON coordinate object"""

    coordinates: list[list[float]] = Field(description="List of GeoJSON coordinate")

    def model_post_init(self, __context: Any) -> None:
        values = self.model_dump()
        values["coordinates"] = [Coordinate(**{"coordinate": c}) for c in values["coordinates"]]
        self.__dict__.update(values)


class ApiInfo(BaseModel):
    """API information object"""

    status: str


class Properties(BaseModel, extra="forbid"):
    """GeoJSON Properties object"""

    timestamp: str = Field(frozen=True, description="Timestamp of the data")
    taxi_count: int = Field(frozen=True, description="Number of taxis")
    api_info: ApiInfo = Field(frozen=True, description="API information")


class Feature(BaseModel, extra="ignore"):
    """GeoJSON Feature object"""

    geometry: Geometry = Field(description="Geometry of the GeoJSON object", frozen=True)
    properties: Properties = Field(description="Properties of the GeoJSON object", frozen=True)


class FeatureCollection(BaseModel, extra="ignore"):
    """GeoJSON FeatureCollection object"""

    features: list[Feature] = Field(description="List of Feature objects", frozen=True)
