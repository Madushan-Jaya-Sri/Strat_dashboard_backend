"""
Authentication Manager for Google OAuth
Handles authentication flow for both Google Ads and Analytics
"""

import os
import jwt
import secrets
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from fastapi import HTTPException
from fastapi.responses import RedirectResponse
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from dotenv import load_dotenv

load_dotenv(override=True)

logger = logging.getLogger(__name__)

class AuthManager:
    """Manages authentication and session handling"""
    
    def __init__(self):
        # OAuth Configuration
        self.GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
        self.GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
        self.REDIRECT_URI = os.getenv("REDIRECT_URI", "http://localhost:8000/auth/callback")
        self.JWT_SECRET = os.getenv("JWT_SECRET", "your-super-secret-jwt-key-change-this")
        
        # Validate configuration
        if not self.GOOGLE_CLIENT_ID or not self.GOOGLE_CLIENT_SECRET:
            logger.error("❌ Missing OAuth credentials!")
            raise ValueError("GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET must be set")
        
        # Combined scopes for both Ads and Analytics
        self.SCOPES = [
            'openid',
            'https://www.googleapis.com/auth/userinfo.email',
            'https://www.googleapis.com/auth/userinfo.profile',
            'https://www.googleapis.com/auth/adwords',  # Google Ads
            'https://www.googleapis.com/auth/analytics.readonly',  # GA4
            'https://www.googleapis.com/auth/analytics.manage.users.readonly'
        ]
        
        # In-memory session storage (use Redis in production)
        self.user_sessions: Dict[str, Dict[str, Any]] = {}
        self.oauth_states: Dict[str, str] = {}
        
        logger.info(f"✅ AuthManager initialized with Client ID: {self.GOOGLE_CLIENT_ID[:20]}...")
    
    def create_jwt_token(self, user_info: dict) -> str:
        """Create JWT token for user session"""
        payload = {
            "email": user_info["email"],
            "name": user_info["name"],
            "picture": user_info["picture"],
            "exp": datetime.utcnow() + timedelta(hours=24)
        }
        return jwt.encode(payload, self.JWT_SECRET, algorithm="HS256")
    
    def verify_jwt_token(self, token: str) -> dict:
        """Verify JWT token and return user info"""
        try:
            payload = jwt.decode(token, self.JWT_SECRET, algorithms=["HS256"])
            return payload
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Token expired")
        except jwt.InvalidTokenError:
            raise HTTPException(status_code=401, detail="Invalid token")
    
    async def initiate_login(self):
        """Initiate Google OAuth login"""
        try:
            flow = Flow.from_client_config(
                {
                    "web": {
                        "client_id": self.GOOGLE_CLIENT_ID,
                        "client_secret": self.GOOGLE_CLIENT_SECRET,
                        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                        "token_uri": "https://oauth2.googleapis.com/token",
                        "redirect_uris": [self.REDIRECT_URI]
                    }
                },
                scopes=self.SCOPES
            )
            flow.redirect_uri = self.REDIRECT_URI
            
            authorization_url, state = flow.authorization_url(
                access_type='offline',
                include_granted_scopes='true',
                prompt='consent'
            )
            
            # Store state for validation
            self.oauth_states[state] = datetime.now().isoformat()
            
            logger.info(f"Generated OAuth URL for login with state: {state}")
            return {"auth_url": authorization_url, "state": state}
            
        except Exception as e:
            logger.error(f"Error generating OAuth URL: {e}")
            raise HTTPException(status_code=500, detail="Failed to generate authorization URL")
    
    async def handle_callback(self, code: str, state: Optional[str] = None):
        """Handle OAuth callback"""
        try:
            # Validate state
            if state and state not in self.oauth_states:
                raise HTTPException(status_code=400, detail="Invalid state parameter")
            
            # Clean up old states
            if state:
                del self.oauth_states[state]
            
            flow = Flow.from_client_config(
                {
                    "web": {
                        "client_id": self.GOOGLE_CLIENT_ID,
                        "client_secret": self.GOOGLE_CLIENT_SECRET,
                        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                        "token_uri": "https://oauth2.googleapis.com/token",
                        "redirect_uris": [self.REDIRECT_URI]
                    }
                },
                scopes=self.SCOPES
            )
            flow.redirect_uri = self.REDIRECT_URI
            
            # Exchange code for tokens
            flow.fetch_token(code=code)
            credentials = flow.credentials
            
            # Get user info
            user_service = build('oauth2', 'v2', credentials=credentials)
            user_info = user_service.userinfo().get().execute()
            
            # Store credentials and user info
            self.user_sessions[user_info['email']] = {
                'credentials': {
                    'token': credentials.token,
                    'refresh_token': credentials.refresh_token,
                    'token_uri': credentials.token_uri,
                    'client_id': credentials.client_id,
                    'client_secret': credentials.client_secret,
                    'scopes': credentials.scopes
                },
                'user_info': user_info,
                'created_at': datetime.now().isoformat()
            }
            
            # Create JWT token
            jwt_token = self.create_jwt_token(user_info)
            
            logger.info(f"User {user_info['email']} authenticated successfully")
            
            # Return token for frontend to handle
            return {"token": jwt_token, "user": user_info}
            
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            raise HTTPException(status_code=400, detail=f"Authentication failed: {str(e)}")
    
    async def logout_user(self, user_email: str):
        """Logout user"""
        if user_email in self.user_sessions:
            del self.user_sessions[user_email]
            logger.info(f"User {user_email} logged out")
        return {"message": "Logged out successfully"}
    
    def get_user_credentials(self, user_email: str) -> Credentials:
        """Get user credentials for API calls"""
        if user_email not in self.user_sessions:
            raise HTTPException(status_code=401, detail="User not authenticated")
        
        creds_data = self.user_sessions[user_email]['credentials']
        credentials = Credentials.from_authorized_user_info(creds_data)
        
        # Refresh if needed
        if credentials.expired and credentials.refresh_token:
            try:
                from google.auth.transport.requests import Request
                credentials.refresh(Request())
                
                # Update stored credentials
                self.user_sessions[user_email]['credentials'].update({
                    'token': credentials.token
                })
                logger.info(f"Refreshed credentials for {user_email}")
            except Exception as e:
                logger.error(f"Failed to refresh credentials: {e}")
                raise HTTPException(status_code=401, detail="Failed to refresh authentication")
        
        return credentials
    
    def get_user_session(self, user_email: str) -> Dict[str, Any]:
        """Get user session data"""
        if user_email not in self.user_sessions:
            raise HTTPException(status_code=401, detail="User not authenticated")
        return self.user_sessions[user_email]