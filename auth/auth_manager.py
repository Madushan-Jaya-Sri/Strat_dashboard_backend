"""
Authentication Manager for Google and Facebook OAuth
Handles authentication flow for Google Ads, Analytics, and Facebook/Meta
"""

import os
import jwt
import secrets
import logging
import requests
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
    """Manages authentication and session handling for Google and Facebook"""
    
    def __init__(self):
        # Google OAuth Configuration
        self.GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
        self.GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
        self.GOOGLE_REDIRECT_URI = os.getenv("REDIRECT_URI", "http://localhost:8000/auth/callback")
        
        # Facebook OAuth Configuration
        self.FACEBOOK_APP_ID = os.getenv("FACEBOOK_APP_ID")
        self.FACEBOOK_APP_SECRET = os.getenv("FACEBOOK_APP_SECRET")
        self.FACEBOOK_REDIRECT_URI = os.getenv("FACEBOOK_REDIRECT_URI", "http://localhost:8000/auth/facebook/callback")
        
        # JWT Configuration
        self.JWT_SECRET = os.getenv("JWT_SECRET", "your-super-secret-jwt-key-change-this")
        
        # Validate Google configuration
        if not self.GOOGLE_CLIENT_ID or not self.GOOGLE_CLIENT_SECRET:
            logger.error("❌ Missing Google OAuth credentials!")
            raise ValueError("GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET must be set")
        
        # Validate Facebook configuration
        if not self.FACEBOOK_APP_ID or not self.FACEBOOK_APP_SECRET:
            logger.warning("⚠️ Missing Facebook OAuth credentials!")
            # Don't raise error - Facebook auth is optional
        
        # Google scopes
        self.GOOGLE_SCOPES = [
            'openid',
            'https://www.googleapis.com/auth/userinfo.email',
            'https://www.googleapis.com/auth/userinfo.profile',
            'https://www.googleapis.com/auth/adwords',  # Google Ads
            'https://www.googleapis.com/auth/analytics.readonly',  # GA4
            'https://www.googleapis.com/auth/analytics.manage.users.readonly'
        ]
        
        # # Facebook scopes
        # self.FACEBOOK_SCOPES = [
        #     'email',
        #     'public_profile',
        #     # Advanced scopes (require app review):
        #     # 'ads_read',
        #     # 'pages_read_engagement',
        #     # 'instagram_basic'
        # ]

        self.FACEBOOK_SCOPES = [
            'email',
            'public_profile',
            'pages_show_list',           # ADD THIS - Required to see pages
            'pages_read_engagement',     # ADD THIS - Required to read page data
            'instagram_basic'            # ADD THIS - For Instagram Business accounts
        ]
        
        # In-memory session storage (use Redis in production)
        self.user_sessions: Dict[str, Dict[str, Any]] = {}
        self.facebook_sessions: Dict[str, Dict[str, Any]] = {}
        self.oauth_states: Dict[str, str] = {}
        self.facebook_states: Dict[str, str] = {}
        
        logger.info(f"✅ AuthManager initialized")
        logger.info(f"📱 Google Client ID: {self.GOOGLE_CLIENT_ID[:20]}...")
        if self.FACEBOOK_APP_ID:
            logger.info(f"📘 Facebook App ID: {self.FACEBOOK_APP_ID}")
    
    def create_jwt_token(self, user_info: dict, auth_provider: str = "google") -> str:
        """Create JWT token for user session"""
        payload = {
            "email": user_info["email"],
            "name": user_info["name"],
            "picture": user_info.get("picture", ""),
            "auth_provider": auth_provider,
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
    
    # =============================================================================
    # GOOGLE AUTHENTICATION METHODS
    # =============================================================================
    
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
                        "redirect_uris": [self.GOOGLE_REDIRECT_URI]
                    }
                },
                scopes=self.GOOGLE_SCOPES
            )
            flow.redirect_uri = self.GOOGLE_REDIRECT_URI
            
            authorization_url, state = flow.authorization_url(
                access_type='offline',
                include_granted_scopes='true',
                prompt='consent'
            )
            
            # Store state for validation
            self.oauth_states[state] = datetime.now().isoformat()
            
            logger.info(f"Generated Google OAuth URL with state: {state}")
            return {"auth_url": authorization_url, "state": state}
            
        except Exception as e:
            logger.error(f"Error generating Google OAuth URL: {e}")
            raise HTTPException(status_code=500, detail="Failed to generate authorization URL")
    
    async def handle_callback(self, code: str, state: Optional[str] = None):
        """Handle Google OAuth callback"""
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
                        "redirect_uris": [self.GOOGLE_REDIRECT_URI]
                    }
                },
                scopes=self.GOOGLE_SCOPES
            )
            flow.redirect_uri = self.GOOGLE_REDIRECT_URI
            
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
                'auth_provider': 'google',
                'created_at': datetime.now().isoformat()
            }
            
            # Create JWT token
            jwt_token = self.create_jwt_token(user_info, "google")
            
            logger.info(f"Google user {user_info['email']} authenticated successfully")
            
            # Return token for frontend to handle
            return {"token": jwt_token, "user": user_info, "auth_provider": "google"}
            
        except Exception as e:
            logger.error(f"Google authentication error: {e}")
            raise HTTPException(status_code=400, detail=f"Authentication failed: {str(e)}")
    
    # =============================================================================
    # FACEBOOK AUTHENTICATION METHODS
    # =============================================================================
    
    async def initiate_facebook_login(self):
        """Initiate Facebook OAuth login"""
        try:
            if not self.FACEBOOK_APP_ID or not self.FACEBOOK_APP_SECRET:
                raise HTTPException(status_code=500, detail="Facebook authentication not configured")
            
            # Generate state for security
            state = secrets.token_urlsafe(32)
            self.facebook_states[state] = datetime.now().isoformat()
            
            # ADD YOUR CONFIG_ID HERE (get this from your Business Configuration)
            config_id = os.getenv("FACEBOOK_CONFIG_ID", "YOUR_CONFIG_ID_FROM_BUSINESS_CONFIGURATION")
            
            # Build Facebook OAuth URL with config_id
            scopes = ",".join(self.FACEBOOK_SCOPES)
            auth_url = (
                f"https://www.facebook.com/v18.0/dialog/oauth?"
                f"client_id={self.FACEBOOK_APP_ID}&"
                f"redirect_uri={self.FACEBOOK_REDIRECT_URI}&"
                f"config_id={config_id}&"  # THIS IS CRUCIAL
                f"scope={scopes}&"
                f"state={state}&"
                f"response_type=code"
            )
            
            logger.info(f"Generated Facebook OAuth URL with config_id: {config_id}")
            return {"auth_url": auth_url, "state": state}
            
        except Exception as e:
            logger.error(f"Error generating Facebook OAuth URL: {e}")
            raise HTTPException(status_code=500, detail="Failed to generate Facebook authorization URL")
    
    async def handle_facebook_callback(self, code: str, state: Optional[str] = None):
        """Handle Facebook OAuth callback"""
        try:
            # Validate state
            if state and state not in self.facebook_states:
                raise HTTPException(status_code=400, detail="Invalid state parameter")
            
            # Clean up old states
            if state:
                del self.facebook_states[state]
            
            # Exchange code for access token
            token_url = "https://graph.facebook.com/v18.0/oauth/access_token"
            token_params = {
                "client_id": self.FACEBOOK_APP_ID,
                "client_secret": self.FACEBOOK_APP_SECRET,
                "redirect_uri": self.FACEBOOK_REDIRECT_URI,
                "code": code
            }
            
            token_response = requests.post(token_url, params=token_params)
            if token_response.status_code != 200:
                logger.error(f"Facebook token exchange failed: {token_response.text}")
                raise HTTPException(status_code=400, detail="Failed to exchange code for token")
            
            token_data = token_response.json()
            access_token = token_data.get("access_token")
            
            if not access_token:
                raise HTTPException(status_code=400, detail="No access token received from Facebook")
            
            # Get user info from Facebook
            user_url = f"https://graph.facebook.com/v18.0/me?fields=id,name,email,picture&access_token={access_token}"
            user_response = requests.get(user_url)
            
            if user_response.status_code != 200:
                logger.error(f"Facebook user info request failed: {user_response.text}")
                raise HTTPException(status_code=400, detail="Failed to get user info from Facebook")
            
            user_info = user_response.json()
                
            # Format user info to match our structure
            formatted_user_info = {
                "id": user_info.get("id"),
                "email": user_info.get("email", ""),
                "name": user_info.get("name", ""),
                "picture": user_info.get("picture", {}).get("data", {}).get("url", "")
            }
            
            # FIX: Use consistent email/ID for both session and JWT
            user_email = formatted_user_info["email"]
            if not user_email:
                # Use Facebook ID as email for consistency
                user_email = f"facebook_{user_info['id']}"
                formatted_user_info["email"] = user_email  # UPDATE THE EMAIL IN USER INFO
            
            # Store Facebook session
            self.facebook_sessions[user_email] = {
                'access_token': access_token,
                'token_type': token_data.get("token_type", "bearer"),
                'expires_in': token_data.get("expires_in"),
                'user_info': formatted_user_info,
                'auth_provider': 'facebook',
                'created_at': datetime.now().isoformat()
            }
            
            # Create JWT token with the SAME email used for session storage
            jwt_token = self.create_jwt_token(formatted_user_info, "facebook")
            
            logger.info(f"Facebook user {user_email} authenticated successfully")
            
            return {"token": jwt_token, "user": formatted_user_info, "auth_provider": "facebook"}
            
        except Exception as e:
            logger.error(f"Facebook authentication error: {e}")
            raise HTTPException(status_code=400, detail=f"Facebook authentication failed: {str(e)}")
    
    # =============================================================================
    # LOGOUT AND SESSION MANAGEMENT
    # =============================================================================
    
    async def logout_user(self, user_email: str, auth_provider: str = "google"):
        """Logout user"""
        if auth_provider == "google" and user_email in self.user_sessions:
            del self.user_sessions[user_email]
            logger.info(f"Google user {user_email} logged out")
        elif auth_provider == "facebook" and user_email in self.facebook_sessions:
            del self.facebook_sessions[user_email]
            logger.info(f"Facebook user {user_email} logged out")
        
        return {"message": "Logged out successfully", "auth_provider": auth_provider}
    
    # =============================================================================
    # CREDENTIAL RETRIEVAL METHODS
    # =============================================================================
    
    def get_user_credentials(self, user_email: str) -> Credentials:
        """Get Google user credentials for API calls"""
        if user_email not in self.user_sessions:
            raise HTTPException(status_code=401, detail="Google user not authenticated")
        
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
                logger.info(f"Refreshed Google credentials for {user_email}")
            except Exception as e:
                logger.error(f"Failed to refresh Google credentials: {e}")
                raise HTTPException(status_code=401, detail="Failed to refresh authentication")
        
        return credentials
    
    def get_facebook_access_token(self, user_email: str) -> str:
        """Get Facebook access token for API calls"""
        if user_email not in self.facebook_sessions:
            raise HTTPException(status_code=401, detail="Facebook user not authenticated")
        
        session = self.facebook_sessions[user_email]
        access_token = session.get('access_token')
        
        if not access_token:
            raise HTTPException(status_code=401, detail="No Facebook access token available")
        
        # TODO: Add token refresh logic when implementing long-lived tokens
        
        return access_token
    
    def get_user_session(self, user_email: str, auth_provider: str = "google") -> Dict[str, Any]:
        """Get user session data"""
        if auth_provider == "google":
            if user_email not in self.user_sessions:
                raise HTTPException(status_code=401, detail="Google user not authenticated")
            return self.user_sessions[user_email]
        elif auth_provider == "facebook":
            if user_email not in self.facebook_sessions:
                raise HTTPException(status_code=401, detail="Facebook user not authenticated")
            return self.facebook_sessions[user_email]
        else:
            raise HTTPException(status_code=400, detail="Invalid auth provider")
    
    # =============================================================================
    # FACEBOOK DEAUTHORIZATION (Required by Facebook)
    # =============================================================================
    
    async def handle_facebook_deauthorization(self, signed_request: str):
        """Handle Facebook app deauthorization"""
        try:
            # Parse signed request to get user ID
            # This is required by Facebook when users revoke app permissions
            logger.info("Facebook app deauthorization request received")
            
            # In production, you should:
            # 1. Parse the signed_request parameter
            # 2. Extract user ID
            # 3. Remove user data from your systems
            # 4. Log the deauthorization
            
            return {"status": "success", "message": "Deauthorization processed"}
            
        except Exception as e:
            logger.error(f"Facebook deauthorization error: {e}")
            return {"status": "error", "message": "Deauthorization failed"}