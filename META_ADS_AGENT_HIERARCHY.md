# Meta Ads Agent Hierarchy - Complete Flow

## Overview
The Meta Ads agent uses a hierarchical approach to navigate through:
- Account Level → Campaign Level → AdSet Level → Ad Level

## Agent Flow

### Agent 1: Intent Classification
**Purpose**: Determine if the question is analytical or chitchat

**Decision**:
- If `ANALYTICAL` → Proceed to Agent 2
- If `CHITCHAT` → Send to LLM for direct response

**Example Inputs**:
- "What's my overall Meta ads performance?" → ANALYTICAL
- "Hi, how are you?" → CHITCHAT

---

### Agent 2: Parameter Extraction
**Purpose**: Extract time period and other parameters from the question

**Extracts**:
- `start_date`: YYYY-MM-DD
- `end_date`: YYYY-MM-DD
- `period`: LAST_7_DAYS, LAST_30_DAYS, etc.
- `entities_mentioned`: Campaign names, keywords, etc.

**Example**:
- Question: "Show me Merdeka Campaign performance in October"
- Extracts: start_date="2025-10-01", end_date="2025-10-31"

---

### Agent 3: Granularity Detection
**Purpose**: Determine if the question is about account-level or specific campaigns/adsets/ads

**Decision Tree**:

#### If ACCOUNT-LEVEL (no specific campaigns mentioned):
```
Trigger: GET /api/meta/ad-accounts/{account_id}/insights/summary
Params: account_id, period, start_date, end_date

Response Example:
{
  "total_spend": 1095.44,
  "total_impressions": 328071,
  "total_clicks": 8258,
  "total_conversions": 0,
  "total_reach": 195116,
  "avg_cpc": 0.132652,
  "avg_cpm": 3.339033,
  "avg_ctr": 2.517138,
  "avg_frequency": 1.681415
}

→ Send to LLM → Generate response → DONE
```

#### If SPECIFIC campaigns/adsets/ads mentioned:
```
Trigger: GET /api/meta/ad-accounts/{account_id}/campaigns/list
Params: account_id, status (optional)

Response Example:
{
  "account_id": "act_303894480866908",
  "total_campaigns": 249,
  "campaigns": [
    {
      "id": "120233799247260711",
      "name": "Merdeka Awareness Campaign | Oct 2025",
      "status": "ACTIVE",
      "objective": "OUTCOME_AWARENESS"
    },
    ...
  ]
}

→ Show DROPDOWN to user → Wait for selection → Proceed to Agent 4
```

**Dropdown Format for Frontend**:
```json
{
  "response": "I found 249 campaigns in your account. Please select the campaign(s) you'd like to analyze.",
  "endpoint_data": {
    "requires_selection": {
      "type": "campaigns",
      "options": [
        {"id": "120233799247260711", "name": "Merdeka Awareness Campaign | Oct 2025", "status": "ACTIVE", "objective": "OUTCOME_AWARENESS"},
        ...
      ],
      "prompt": "I found 249 campaigns. Please select the campaign(s) you'd like to analyze."
    }
  }
}
```

---

### Agent 4: Campaign-Level Decision
**Input**: Selected campaign IDs from user

**Purpose**: Determine if question is about campaign-level metrics or needs deeper drill-down to adsets

**Decision Tree**:

#### If CAMPAIGN-LEVEL ONLY:
```
Question indicators: "campaign performance", "campaign metrics", "campaign ROI"

Trigger ONE OR MORE of:
1. POST /api/meta/campaigns/timeseries
   Body: {"campaign_ids": ["123", "456"]}
   Params: start_date, end_date, period

2. POST /api/meta/campaigns/demographics
   Body: {"campaign_ids": ["123", "456"]}
   Params: start_date, end_date, period

3. POST /api/meta/campaigns/placements
   Body: {"campaign_ids": ["123", "456"]}
   Params: start_date, end_date, period

→ Send responses to LLM → Generate insights → DONE
```

#### If NEEDS ADSET DRILL-DOWN:
```
Question indicators: "adsets", "ad sets", "audience", "targeting"

Trigger: POST /api/meta/campaigns/adsets
Body: {"campaign_ids": ["120227030122080711", "120231099841830711"]}

Response Example:
[
  {
    "id": "120227030122600711",
    "name": "Merdeka IG traffic campaign | July",
    "campaign_id": "120227030122080711",
    "status": "ACTIVE",
    "optimization_goal": "PROFILE_VISIT",
    "locations": ["MY"],
    "lifetime_budget": 1500.0
  },
  ...
]

→ Show DROPDOWN to user → Wait for selection → Proceed to Agent 5
```

**Dropdown Format**:
```json
{
  "endpoint_data": {
    "requires_selection": {
      "type": "adsets",
      "options": [
        {"id": "120227030122600711", "name": "Merdeka IG traffic campaign | July", "status": "ACTIVE"},
        ...
      ],
      "prompt": "Select the adset(s) you want to analyze."
    }
  }
}
```

---

### Agent 5: AdSet-Level Decision
**Input**: Selected adset IDs from user

**Purpose**: Determine if question is about adset-level metrics or needs deeper drill-down to ads

**Decision Tree**:

#### If ADSET-LEVEL ONLY:
```
Question indicators: "adset performance", "audience performance", "targeting metrics"

Trigger ONE OR MORE of:
1. POST /api/meta/adsets/timeseries
   Body: {"adset_ids": ["123", "456"]}
   Params: start_date, end_date, period

2. POST /api/meta/adsets/demographics
   Body: {"adset_ids": ["123", "456"]}
   Params: start_date, end_date, period

3. POST /api/meta/adsets/placements
   Body: {"adset_ids": ["123", "456"]}
   Params: start_date, end_date, period

→ Send responses to LLM → Generate insights → DONE
```

#### If NEEDS AD DRILL-DOWN:
```
Question indicators: "ads", "creatives", "ad copy", "specific ads"

Trigger: POST /api/meta/adsets/ads
Body: {"adset_ids": ["120231547125090711", "120231479556210711"]}

Response Example:
[
  {
    "id": "120231774406730711",
    "name": "Datuk Seri A Samad",
    "ad_set_id": "120231547125090711",
    "status": "PAUSED",
    "creative": {
      "body": "...",
      "media_url": "https://..."
    },
    "preview_link": "https://fb.me/..."
  },
  ...
]

→ Show DROPDOWN to user → Wait for selection → Proceed to Agent 6
```

**Dropdown Format**:
```json
{
  "endpoint_data": {
    "requires_selection": {
      "type": "ads",
      "options": [
        {"id": "120231774406730711", "name": "Datuk Seri A Samad", "status": "PAUSED"},
        ...
      ],
      "prompt": "Select the ad(s) you want to analyze."
    }
  }
}
```

---

### Agent 6: Ad-Level Analysis
**Input**: Selected ad IDs from user

**Purpose**: Analyze specific ad performance

**Actions**:
```
Trigger ONE OR MORE of:
1. POST /api/meta/ads/timeseries
   Body: {"ad_ids": ["123", "456"]}
   Params: start_date, end_date, period

2. POST /api/meta/ads/demographics
   Body: {"ad_ids": ["123", "456"]}
   Params: start_date, end_date, period

3. POST /api/meta/ads/placements
   Body: {"ad_ids": ["123", "456"]}
   Params: start_date, end_date, period

→ Send responses to LLM → Generate insights → DONE
```

**If Still Unclear**:
```
Ask for clarification: "Could you please specify which campaigns, adsets, or ads you'd like me to analyze?"
→ Return to appropriate level based on user's clarification
```

---

## Frontend Dropdown Rendering

The frontend should check for `response.endpoint_data.requires_selection` and render a dropdown when present:

```javascript
if (response.endpoint_data?.requires_selection) {
  const { type, options, prompt } = response.endpoint_data.requires_selection;

  // Render dropdown based on type
  if (type === 'campaigns') {
    // Show campaign selection dropdown
  } else if (type === 'adsets') {
    // Show adset selection dropdown
  } else if (type === 'ads') {
    // Show ad selection dropdown
  }
}
```

## Example Flow

### Scenario 1: Account-Level Question
```
User: "What's my overall Meta ads performance?"
→ Agent 1: ANALYTICAL
→ Agent 2: Extract time period (default to module period)
→ Agent 3: ACCOUNT-LEVEL
→ Call: GET /api/meta/ad-accounts/{id}/insights/summary
→ LLM generates response
→ User sees: "Your Meta ads performance summary..."
```

### Scenario 2: Campaign-Specific Question
```
User: "Show me Merdeka Traffic Campaign performance"
→ Agent 1: ANALYTICAL
→ Agent 2: Extract: entities=["Merdeka Traffic Campaign"]
→ Agent 3: SPECIFIC → Load campaigns
→ Frontend shows: Dropdown with 249 campaigns
→ User selects: "Merdeka Traffic Campaign | 7 categories | Oct - Dec"
→ Agent 4: CAMPAIGN-LEVEL
→ Calls: POST /api/meta/campaigns/timeseries + demographics
→ LLM generates insights
→ User sees: "The Merdeka Traffic Campaign performance..."
```

### Scenario 3: Ad-Level Question
```
User: "Which ads in my Merdeka campaign are performing best?"
→ Agent 1: ANALYTICAL
→ Agent 2: Extract: entities=["Merdeka campaign"], indicators=["ads"]
→ Agent 3: SPECIFIC → Load campaigns
→ Frontend: Dropdown with campaigns
→ User selects: "Merdeka Campaign"
→ Agent 4: NEEDS ADSET DRILL-DOWN → Load adsets
→ Frontend: Dropdown with adsets
→ User selects: Multiple adsets
→ Agent 5: NEEDS AD DRILL-DOWN → Load ads
→ Frontend: Dropdown with ads
→ User selects: Specific ads or "all"
→ Agent 6: AD-LEVEL ANALYSIS
→ Calls: POST /api/meta/ads/timeseries + demographics + placements
→ LLM generates insights
→ User sees: "Your top performing ads are..."
```

---

## Implementation Checklist

- [x] Agent 1: Intent Classification
- [x] Agent 2: Parameter Extraction
- [x] Agent 3: Granularity Detection
- [x] Agent 4: Campaign-Level Decision
- [x] Agent 5: AdSet-Level Decision
- [x] Agent 6: Ad-Level Analysis
- [x] Dropdown data structure for campaigns
- [x] Dropdown data structure for adsets
- [x] Dropdown data structure for ads
- [x] Internal API calls to avoid timeouts
- [x] Async-to-sync bridge with ThreadPoolExecutor
- [x] Error handling with user-friendly messages
- [ ] Frontend dropdown rendering (needs verification)
- [ ] End-to-end testing

---

## Testing Commands

### Test Account-Level:
```
"What's my overall Meta ads performance?"
Expected: Direct response with account metrics
```

### Test Campaign-Level:
```
"Show me the Merdeka Traffic Campaign performance"
Expected: Dropdown with campaigns → Select → Campaign insights
```

### Test AdSet-Level:
```
"Which audiences are performing best in my Merdeka campaign?"
Expected: Campaign dropdown → AdSet dropdown → AdSet insights
```

### Test Ad-Level:
```
"Show me the best performing ads in my Merdeka campaign"
Expected: Campaign dropdown → AdSet dropdown → Ad dropdown → Ad insights
```
