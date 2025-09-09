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
        self.FACEBOOK_CONFIG_ID = os.getenv("FACEBOOK_CONFIG_ID", "")
        
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
        
        self.FACEBOOK_SCOPES = [
            'email',
            'public_profile',
            'pages_show_list',           # Required to see pages
            'pages_read_engagement',     # Required to read page data
            'instagram_basic'            # For Instagram Business accounts
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
            logger.info(f"📘 Facebook Config ID: {self.FACEBOOK_CONFIG_ID}")
            logger.info(f"📘 Facebook Redirect URI: {self.FACEBOOK_REDIRECT_URI}")
            logger.info(f"📘 Facebook Scopes: {', '.join(self.FACEBOOK_SCOPES)}")
    
    def create_jwt_token(self, user_info: dict, auth_provider: str = "google") -> str:
        """Create JWT token for user session"""
        logger.info(f"🔑 Creating JWT token for {auth_provider} user: {user_info.get('email', 'no-email')}")
        payload = {
            "email": user_info["email"],
            "name": user_info["name"],
            "picture": user_info.get("picture", ""),
            "auth_provider": auth_provider,
            "exp": datetime.utcnow() + timedelta(hours=24)
        }
        token = jwt.encode(payload, self.JWT_SECRET, algorithm="HS256")
        logger.info(f"✅ JWT token created successfully: {token[:50]}...")
        return token
    
    def verify_jwt_token(self, token: str) -> dict:
        """Verify JWT token and return user info"""
        try:
            payload = jwt.decode(token, self.JWT_SECRET, algorithms=["HS256"])
            logger.info(f"✅ JWT token verified for user: {payload.get('email', 'unknown')}")
            return payload
        except jwt.ExpiredSignatureError:
            logger.error("❌ JWT token expired")
            raise HTTPException(status_code=401, detail="Token expired")
        except jwt.InvalidTokenError:
            logger.error("❌ Invalid JWT token")
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
        """Initiate Facebook OAuth login with comprehensive debugging"""
        logger.info("🚀 FACEBOOK LOGIN INITIATION STARTED")
        logger.info("=" * 60)
        
        try:
            # Step 1: Validate configuration
            logger.info("📋 Step 1: Validating Facebook configuration...")
            if not self.FACEBOOK_APP_ID or not self.FACEBOOK_APP_SECRET:
                logger.error("❌ Missing Facebook credentials")
                logger.error(f"   - App ID: {'SET' if self.FACEBOOK_APP_ID else 'MISSING'}")
                logger.error(f"   - App Secret: {'SET' if self.FACEBOOK_APP_SECRET else 'MISSING'}")
                raise HTTPException(status_code=500, detail="Facebook authentication not configured")
            
            logger.info("✅ Facebook credentials validated")
            logger.info(f"   - App ID: {self.FACEBOOK_APP_ID}")
            logger.info(f"   - Redirect URI: {self.FACEBOOK_REDIRECT_URI}")
            logger.info(f"   - Config ID: {self.FACEBOOK_CONFIG_ID if self.FACEBOOK_CONFIG_ID else 'NOT SET'}")
            
            # Step 2: Generate security state
            logger.info("📋 Step 2: Generating security state...")
            state = secrets.token_urlsafe(32)
            self.facebook_states[state] = datetime.now().isoformat()
            logger.info(f"✅ State generated: {state}")
            logger.info(f"   - Stored states count: {len(self.facebook_states)}")
            
            # Step 3: Prepare OAuth URL parameters
            logger.info("📋 Step 3: Building OAuth URL...")
            scopes = ",".join(self.FACEBOOK_SCOPES)
            logger.info(f"   - Scopes: {scopes}")
            
            # Build OAuth URL with or without config_id
            base_url = "https://www.facebook.com/v18.0/dialog/oauth"
            params = {
                "client_id": self.FACEBOOK_APP_ID,
                "redirect_uri": self.FACEBOOK_REDIRECT_URI,
                "scope": scopes,
                "state": state,
                "response_type": "code"
            }
            
            # Add config_id if available
            # if self.FACEBOOK_CONFIG_ID:
            #     params["config_id"] = self.FACEBOOK_CONFIG_ID
            #     logger.info(f"   - Including config_id: {self.FACEBOOK_CONFIG_ID}")
            # else:
            #     logger.warning("⚠️  No config_id provided - this may limit external user access")
            
            # Construct final URL
            auth_url = base_url + "?" + "&".join([f"{k}={v}" for k, v in params.items()])
            
            logger.info("✅ OAuth URL constructed successfully")
            logger.info(f"   - Full URL: {auth_url}")
            logger.info(f"   - URL length: {len(auth_url)} characters")
            
            # Step 4: Final validation
            logger.info("📋 Step 4: Final validation...")
            logger.info(f"   - State stored: {state in self.facebook_states}")
            logger.info(f"   - App ID length: {len(self.FACEBOOK_APP_ID)}")
            logger.info(f"   - Redirect URI valid: {self.FACEBOOK_REDIRECT_URI.startswith('http')}")
            
            result = {"auth_url": auth_url, "state": state}
            logger.info("🎉 FACEBOOK LOGIN INITIATION COMPLETED SUCCESSFULLY")
            logger.info("=" * 60)
            
            return result
            
        except Exception as e:
            logger.error("💥 FACEBOOK LOGIN INITIATION FAILED")
            logger.error(f"❌ Error type: {type(e).__name__}")
            logger.error(f"❌ Error message: {str(e)}")
            logger.error("=" * 60)
            raise HTTPException(status_code=500, detail=f"Failed to generate Facebook authorization URL: {str(e)}")
    
    async def handle_facebook_callback(self, code: str, state: Optional[str] = None):
        """Handle Facebook OAuth callback with comprehensive debugging"""
        logger.info("🔄 FACEBOOK CALLBACK PROCESSING STARTED")
        logger.info("=" * 60)
        logger.info(f"📨 Received callback with:")
        logger.info(f"   - Code: {code[:20]}...{code[-10:] if len(code) > 30 else code}")
        logger.info(f"   - State: {state}")
        
        try:
            # Step 1: Validate state
            logger.info("📋 Step 1: Validating state parameter...")
            if state and state not in self.facebook_states:
                logger.error(f"❌ Invalid state parameter: {state}")
                logger.error(f"   - Available states: {list(self.facebook_states.keys())}")
                raise HTTPException(status_code=400, detail="Invalid state parameter")
            
            if state:
                logger.info(f"✅ State validated: {state}")
                del self.facebook_states[state]
                logger.info(f"   - State cleaned up, remaining: {len(self.facebook_states)}")
            else:
                logger.warning("⚠️  No state parameter provided")
            
            # Step 2: Exchange code for access token
            logger.info("📋 Step 2: Exchanging code for access token...")
            token_url = "https://graph.facebook.com/v18.0/oauth/access_token"
            token_params = {
                "client_id": self.FACEBOOK_APP_ID,
                "client_secret": self.FACEBOOK_APP_SECRET,
                "redirect_uri": self.FACEBOOK_REDIRECT_URI,
                "code": code
            }
            
            logger.info(f"   - Token URL: {token_url}")
            logger.info(f"   - Request params: {list(token_params.keys())}")
            
            token_response = requests.post(token_url, params=token_params)
            logger.info(f"   - Response status: {token_response.status_code}")
            logger.info(f"   - Response headers: {dict(token_response.headers)}")
            
            if token_response.status_code != 200:
                logger.error(f"❌ Token exchange failed:")
                logger.error(f"   - Status: {token_response.status_code}")
                logger.error(f"   - Response: {token_response.text}")
                raise HTTPException(status_code=400, detail="Failed to exchange code for token")
            
            token_data = token_response.json()
            logger.info(f"✅ Token exchange successful")
            logger.info(f"   - Response keys: {list(token_data.keys())}")
            
            access_token = token_data.get("access_token")
            if not access_token:
                logger.error("❌ No access token in response")
                logger.error(f"   - Full response: {token_data}")
                raise HTTPException(status_code=400, detail="No access token received from Facebook")
            
            logger.info(f"✅ Access token obtained: {access_token[:20]}...{access_token[-10:]}")
            
            # Step 3: Get user info from Facebook
            logger.info("📋 Step 3: Fetching user information...")
            user_url = f"https://graph.facebook.com/v18.0/me?fields=id,name,email,picture&access_token={access_token}"
            logger.info(f"   - User info URL: {user_url[:100]}...")
            
            user_response = requests.get(user_url)
            logger.info(f"   - User info response status: {user_response.status_code}")
            
            if user_response.status_code != 200:
                logger.error(f"❌ User info request failed:")
                logger.error(f"   - Status: {user_response.status_code}")
                logger.error(f"   - Response: {user_response.text}")
                raise HTTPException(status_code=400, detail="Failed to get user info from Facebook")
            
            user_info = user_response.json()
            logger.info(f"✅ User info retrieved successfully")
            logger.info(f"   - User ID: {user_info.get('id', 'N/A')}")
            logger.info(f"   - User Name: {user_info.get('name', 'N/A')}")
            logger.info(f"   - User Email: {user_info.get('email', 'N/A')}")
            logger.info(f"   - User fields: {list(user_info.keys())}")
            
            # Step 4: Format user info
            logger.info("📋 Step 4: Formatting user information...")
            formatted_user_info = {
                "id": user_info.get("id"),
                "email": user_info.get("email", ""),
                "name": user_info.get("name", ""),
                "picture": user_info.get("picture", {}).get("data", {}).get("url", "")
            }
            
            # Handle missing email
            user_email = formatted_user_info["email"]
            if not user_email:
                logger.warning("⚠️  No email provided by Facebook, using Facebook ID")
                user_email = f"facebook_{user_info['id']}"
                formatted_user_info["email"] = user_email
            
            logger.info(f"✅ User info formatted")
            logger.info(f"   - Final email: {user_email}")
            logger.info(f"   - Final name: {formatted_user_info['name']}")
            
            # Step 5: Store Facebook session
            logger.info("📋 Step 5: Storing Facebook session...")
            session_data = {
                'access_token': access_token,
                'token_type': token_data.get("token_type", "bearer"),
                'expires_in': token_data.get("expires_in"),
                'user_info': formatted_user_info,
                'auth_provider': 'facebook',
                'created_at': datetime.now().isoformat()
            }
            
            self.facebook_sessions[user_email] = session_data
            logger.info(f"✅ Session stored for user: {user_email}")
            logger.info(f"   - Total Facebook sessions: {len(self.facebook_sessions)}")
            logger.info(f"   - Session keys: {list(session_data.keys())}")
            
            # Step 6: Create JWT token
            logger.info("📋 Step 6: Creating JWT token...")
            jwt_token = self.create_jwt_token(formatted_user_info, "facebook")
            
            # Step 7: Final result
            result = {
                "token": jwt_token, 
                "user": formatted_user_info, 
                "auth_provider": "facebook"
            }
            
            logger.info("🎉 FACEBOOK CALLBACK PROCESSING COMPLETED SUCCESSFULLY")
            logger.info(f"✅ Final result keys: {list(result.keys())}")
            logger.info("=" * 60)
            
            return result
            
        except Exception as e:
            logger.error("💥 FACEBOOK CALLBACK PROCESSING FAILED")
            logger.error(f"❌ Error type: {type(e).__name__}")
            logger.error(f"❌ Error message: {str(e)}")
            logger.error(f"❌ Error details: {repr(e)}")
            
            # Additional debug info for specific errors
            if "HTTPException" in str(type(e)):
                logger.error(f"❌ HTTP Exception details: {e.detail if hasattr(e, 'detail') else 'N/A'}")
            
            logger.error("=" * 60)
            raise HTTPException(status_code=400, detail=f"Facebook authentication failed: {str(e)}")
    
    # =============================================================================
    # LOGOUT AND SESSION MANAGEMENT
    # =============================================================================
    
    async def logout_user(self, user_email: str, auth_provider: str = "google"):
        """Logout user with debugging"""
        logger.info(f"🚪 Logging out {auth_provider} user: {user_email}")
        
        if auth_provider == "google" and user_email in self.user_sessions:
            del self.user_sessions[user_email]
            logger.info(f"✅ Google user {user_email} logged out successfully")
        elif auth_provider == "facebook" and user_email in self.facebook_sessions:
            del self.facebook_sessions[user_email]
            logger.info(f"✅ Facebook user {user_email} logged out successfully")
        else:
            logger.warning(f"⚠️  User {user_email} not found in {auth_provider} sessions")
        
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
        """Get Facebook access token for API calls with debugging"""
        logger.info(f"🔑 Retrieving Facebook access token for: {user_email}")
        logger.info(f"   - Available sessions: {list(self.facebook_sessions.keys())}")
        
        if user_email not in self.facebook_sessions:
            logger.error(f"❌ User {user_email} not found in Facebook sessions")
            logger.error(f"   - Available emails: {list(self.facebook_sessions.keys())}")
            raise HTTPException(status_code=401, detail="Facebook user not authenticated")
        
        session = self.facebook_sessions[user_email]
        access_token = session.get('access_token')
        
        if not access_token:
            logger.error(f"❌ No access token found for user {user_email}")
            logger.error(f"   - Session keys: {list(session.keys())}")
            raise HTTPException(status_code=401, detail="No Facebook access token available")
        
        logger.info(f"✅ Access token retrieved successfully: {access_token[:20]}...")
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
    # DEBUG METHODS
    # =============================================================================
    
    def debug_facebook_sessions(self):
        """Debug method to inspect Facebook sessions"""
        logger.info("🔍 FACEBOOK SESSIONS DEBUG")
        logger.info("=" * 40)
        logger.info(f"Total sessions: {len(self.facebook_sessions)}")
        
        for email, session in self.facebook_sessions.items():
            logger.info(f"📧 User: {email}")
            logger.info(f"   - Created: {session.get('created_at', 'Unknown')}")
            logger.info(f"   - Provider: {session.get('auth_provider', 'Unknown')}")
            logger.info(f"   - Has token: {'access_token' in session}")
            logger.info(f"   - Token preview: {session.get('access_token', 'N/A')[:20]}...")
        
        logger.info("=" * 40)
    
    def debug_facebook_states(self):
        """Debug method to inspect Facebook states"""
        logger.info("🔍 FACEBOOK STATES DEBUG")
        logger.info("=" * 40)
        logger.info(f"Total states: {len(self.facebook_states)}")
        
        for state, timestamp in self.facebook_states.items():
            logger.info(f"🔑 State: {state}")
            logger.info(f"   - Created: {timestamp}")
        
        logger.info("=" * 40)
    
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