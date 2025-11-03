from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum


class ModuleType(str, Enum):
    GOOGLE_ADS = "google_ads"
    GOOGLE_ANALYTICS = "google_analytics"
    INTENT_INSIGHTS = "intent_insights"
    META_ADS = "meta_ads"
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
    account_id: Optional[str] = None
    period: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    context: Optional[Dict[str, Any]] = {}

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


class UserChatDocument(BaseModel):
    """Document structure for storing all user chats"""
    user_email: str
    module_type: str  # 'google_ads', 'google_analytics', 'intent_insights'
    conversations: List[Dict[str, Any]] = []  # List of conversation objects
    created_at: datetime
    last_activity: datetime

class ConversationSession(BaseModel):
    """Individual conversation session within a user document"""
    session_id: str
    customer_id: Optional[str] = None
    property_id: Optional[str] = None
    period: str
    context: Dict[str, Any] = {}
    messages: List[ChatMessage] = []
    created_at: datetime
    last_activity: datetime
    is_active: bool = True

class DocumentSearchCriteria(BaseModel):
    """Criteria for searching documents in MongoDB collections"""
    user_email: str
    customer_id: Optional[str] = None
    property_id: Optional[str] = None
    period: str
    selected_collections: List[str]