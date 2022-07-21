from typing import Optional, List
from pydantic import BaseModel, root_validator
from enum import Enum


class RecommendationCandidate(BaseModel):
    item_id: int
    publisher: str
    feed_id: Optional[int] = None


class CurationProspect(BaseModel):
    scheduled_surface_guid: str
    prospect_id: str
    url: str
    prospect_source: str
    save_count: int
    predicted_topic: str
    rank: int


class CandidateType(str, Enum):
    prospect = "prospect"
    recommendation = "recommendation"


class CandidateSet(BaseModel):
    id: str
    version: int
    candidates: List
    type: CandidateType
    flow: str
    run: str
    expires_at: int

    @root_validator(pre=False)
    def validate_list_elements(cls, values):
        expected_type = CurationProspect if values.get("type") == CandidateType.prospect else RecommendationCandidate
        for c in values.get("candidates"):
            assert type(c) == expected_type, f"expected {expected_type} : got {type(c)}"
        return values
