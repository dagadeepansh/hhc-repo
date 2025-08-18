from pydantic import BaseModel
from typing import List, Optional, Any, Dict

class QueryRequest(BaseModel):
    """Request model for the user query."""
    query: str

# Since the LLM response structure is well-defined, we can model it.
# This adds a layer of validation to the data we receive from the LLM.
class FilterModel(BaseModel):
    AcceptingNewPatients: Optional[bool] = None
    FinancialAssistance: Optional[bool] = None
    Gender: Optional[str] = None
    Insurance: Optional[str]= None
    Language: Optional[str] = None
    Location: Optional[str] = None
    OpenScheduling: Optional[bool] = None
    Organization: Optional[str] = None
    Practice: Optional[str] = None
    Rating: Optional[int] = None
    specialty_rollup: Optional[List[str]] = None
    
class RecommendationModel(BaseModel):
    id: int
    reason:str
    specialty:str
    type:str
    score: Optional[float] = None # Added score field for ranking results

class RecommendationResponse(BaseModel):
    """The final JSON response structure."""
    filters: FilterModel
    recommendations: List[RecommendationModel]