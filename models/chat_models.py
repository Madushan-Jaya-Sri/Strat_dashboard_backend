from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum

class ModuleType(str, Enum):
    GOOGLE_ADS = "google_ads"
    GOOGLE_ANALYTICS = "google_analytics" 
    INTENT_INSIGHTS = "intent_insights"
    META = "meta"
    COMBINED = "combined"

class MessageRole(str, Enum):
    USER = "user"
    ASSISTANT = "assistant"
    SYSTEM = "system"

class ChatMessage(BaseModel):
    role: MessageRole
    content: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    metadata: Optional[Dict[str, Any]] = None

class ChatRequest(BaseModel):
    message: str
    module_type: ModuleType
    session_id: Optional[str] = None
    customer_id: Optional[str] = None
    property_id: Optional[str] = None
    context: Optional[Dict[str, Any]] = None

class ChatResponse(BaseModel):
    response: str
    session_id: str
    triggered_endpoint: Optional[str] = None
    endpoint_data: Optional[Dict[str, Any]] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    module_type: ModuleType

class ChatSession(BaseModel):
    session_id: str
    user_email: str
    module_type: ModuleType
    customer_id: Optional[str] = None
    property_id: Optional[str] = None
    messages: List[ChatMessage] = []
    created_at: datetime = Field(default_factory=datetime.utcnow)
    last_activity: datetime = Field(default_factory=datetime.utcnow)
    is_active: bool = True

class ChatHistoryResponse(BaseModel):
    sessions: List[ChatSession]
    total_sessions: int
    module_type: ModuleType

class DeleteChatRequest(BaseModel):
    session_ids: List[str]