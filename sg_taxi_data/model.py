"""Model classes for the Singapore Taxi Data API"""

from pydantic import BaseModel, Field, field_validator


class Geometry(BaseModel, extra="ignore"):
    """List of GeoJSON coordinate object"""

    coordinates: list[list[float]] = Field(description="List of GeoJSON coordinate")

    @field_validator("coordinates")
    @classmethod
    def validate_coordinate(cls, coordinates):
        """Validate the coordinate field to ensure it is a list of 2 element"""

        for coordinate in coordinates:
            if len(coordinate) != 2:
                raise ValueError("Coordinate must have 2 elements")
        return coordinates


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
