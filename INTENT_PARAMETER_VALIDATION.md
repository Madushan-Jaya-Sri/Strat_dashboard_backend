# Intent Chat - Smart Parameter Validation Flow

## Overview

The Intent chat now intelligently validates all required parameters before triggering the keyword insights endpoint. If any parameter is missing, it asks the user for it with helpful, context-aware prompts.

---

## Required Parameters

The Intent keyword insights endpoint requires **THREE** parameters:

1. **Seed Keywords** - Keywords/topics to research (e.g., "cosmetics", "beauty products", "skincare")
2. **Country/Region** - Geographic location for analysis (e.g., "United States", "Canada", "World Wide")
3. **Time Period** - Timeframe for analysis (e.g., "last 7 days", "last 30 days", "last 3 months")

---

## How the Validation Flow Works

### Step 1: Extract Parameters

When a user asks a question, Agent 2 uses an enhanced LLM prompt to extract:

- **Seed Keywords** from `entities_mentioned`
- **Country** from `filters.country`
- **Time Period** from `period_keyword` or dates

**Example Question:**
```
"Show me trending keywords for organic skincare in the United States for the last 30 days"
```

**Extracted:**
```json
{
  "entities_mentioned": ["organic skincare"],
  "filters": {"country": "United States"},
  "period_keyword": "last 30 days"
}
```

### Step 2: Check Context

If a parameter is not in the question, check if it's available from the Intent module UI:

```python
# Priority Order:
# 1. Extracted from question → Use it
# 2. Available in context (UI) → Use it
# 3. Not available → Ask for it
```

### Step 3: Validate All Three

After extraction and context check, validate that all three parameters are present:

```python
missing_params = []

if not seed_keywords:
    missing_params.append("seed_keywords")
if not country:
    missing_params.append("country")
if not time_period:
    missing_params.append("time_period")
```

### Step 4: Ask for Missing Parameters

If any parameters are missing, ask the user with an intelligent prompt:

#### All 3 Missing
```
To provide keyword insights, I need the following information:

1️⃣ **Seed Keywords**: What keywords or topics would you like to research?
   (e.g., 'cosmetics', 'beauty products')
2️⃣ **Country/Region**: Which country or region to analyze?
   (e.g., 'United States', 'Canada', 'World Wide')
3️⃣ **Time Period**: What time period to analyze?
   (e.g., 'last 7 days', 'last 30 days', 'last 3 months')

Please provide these details.
```

#### Only Seed Keywords Missing
```
What keywords or topics would you like to research?

Please provide seed keywords (e.g., 'digital marketing', 'seo tools', 'content writing').
```

#### Only Country Missing
```
Which country or region would you like to analyze?

Please specify (e.g., 'United States', 'Canada', 'United Kingdom', 'World Wide').
```

#### Only Time Period Missing
```
What time period would you like to analyze?

Please specify (e.g., 'last 7 days', 'last 30 days', 'last 3 months', 'last year').
```

### Step 5: Collect Missing Parameters

The user provides the missing information in their next message. Agent 2 extracts it and validates again.

### Step 6: Trigger Endpoint

Once all three parameters are present, proceed to trigger the keyword insights endpoint.

---

## Example Conversations

### Scenario 1: All Parameters in Question ✅

**User:**
```
Show me trending keywords for coffee makers in Canada for the last 7 days
```

**System:**
- ✅ Seed Keywords: ["coffee makers"] (from question)
- ✅ Country: "Canada" (from question)
- ✅ Time Period: "last 7 days" (from question)
- ✅ **Triggers endpoint immediately**

---

### Scenario 2: Missing Time Period ⏱️

**User:**
```
What are trending keywords for yoga mats in the United States?
```

**System:**
- ✅ Seed Keywords: ["yoga mats"]
- ✅ Country: "United States"
- ❌ Time Period: **Missing**

**AI Response:**
```
What time period would you like to analyze?

Please specify (e.g., 'last 7 days', 'last 30 days', 'last 3 months', 'last year').
```

**User:**
```
Last 30 days
```

**System:**
- ✅ Extracts: "last 30 days"
- ✅ **All parameters present - triggers endpoint**

---

### Scenario 3: Missing Country and Time Period 🌍⏱️

**User:**
```
Show me keyword insights for running shoes
```

**System:**
- ✅ Seed Keywords: ["running shoes"]
- ❌ Country: **Missing**
- ❌ Time Period: **Missing**

**AI Response:**
```
To provide keyword insights, I need:

1️⃣ **Country/Region**: Which country should I analyze?
2️⃣ **Time Period**: What time period to analyze?

Please provide these details.
```

**User:**
```
United States, last 3 months
```

**System:**
- ✅ Extracts: Country = "United States", Period = "last 3 months"
- ✅ **All parameters present - triggers endpoint**

---

### Scenario 4: Using UI Context 🎨

**User Setup in UI:**
- Seed Keywords: ["cosmetics", "beauty", "makeup"]
- Country: "Canada"
- No Time Period selected

**User Question:**
```
What are the trending keywords for the last month?
```

**System:**
- ✅ Seed Keywords: ["cosmetics", "beauty", "makeup"] (from UI)
- ✅ Country: "Canada" (from UI)
- ✅ Time Period: "last month" (from question)
- ✅ **Triggers endpoint immediately**

---

### Scenario 5: General Question with No Parameters ❓

**User:**
```
What are the trending keywords?
```

**System:**
- ❌ Seed Keywords: **Missing**
- ❌ Country: **Missing**
- ❌ Time Period: **Missing**
- ❌ (Also not in UI context)

**AI Response:**
```
To provide keyword insights, I need the following information:

1️⃣ **Seed Keywords**: What keywords or topics would you like to research?
   (e.g., 'cosmetics', 'beauty products')
2️⃣ **Country/Region**: Which country or region to analyze?
   (e.g., 'United States', 'Canada', 'World Wide')
3️⃣ **Time Period**: What time period to analyze?
   (e.g., 'last 7 days', 'last 30 days', 'last 3 months')

Please provide these details.
```

**User:**
```
I want to research digital marketing keywords in the United States for the last 7 days
```

**System:**
- ✅ Seed Keywords: ["digital marketing"]
- ✅ Country: "United States"
- ✅ Time Period: "last 7 days"
- ✅ **Triggers endpoint**

---

## Enhanced LLM Extraction

The parameter extraction now uses a specialized prompt for Intent insights:

### What It Extracts

```python
# Seed Keywords (entities_mentioned)
"trending keywords for shoes" → ["shoes"]
"insights for coffee and tea" → ["coffee", "tea"]
"keyword research digital marketing seo" → ["digital marketing", "seo"]

# Country (filters.country)
"in the United States" → "United States"
"for Canada" → "Canada"
"worldwide" → "World Wide"

# Time Period (period_keyword)
"last 7 days" → "last 7 days"
"past month" → "last month"
"for the last 3 months" → "last 3 months"
```

### Examples

**Input:**
```
"Show me trending keywords for luxury watches in France for the last 30 days"
```

**Output:**
```json
{
  "entities_mentioned": ["luxury watches"],
  "filters": {"country": "France"},
  "period_keyword": "last 30 days",
  "has_time_period": true
}
```

---

## Backend Implementation

### Files Modified

1. **[shared_agents.py:208-245](chat/agents/shared_agents.py#L208-L245)** - Enhanced LLM extraction prompt for Intent
2. **[shared_agents.py:344-466](chat/agents/shared_agents.py#L344-L466)** - Parameter validation logic
3. **[internal_api_caller.py:417](chat/utils/internal_api_caller.py#L417)** - Fixed dict to Pydantic model conversion
4. **[api_client.py:565](chat/utils/api_client.py#L565)** - Fixed customer_id parameter building

### Logic Flow

```python
# Agent 2: Parameter Extraction
if module_type == "intent_insights":
    # Step 1: Extract from question
    if extracted["entities_mentioned"]:
        seed_keywords = extracted["entities_mentioned"]
    elif context has seed_keywords:
        seed_keywords = context["seed_keywords"]

    # Step 2: Extract country
    if extracted["filters"]["country"]:
        country = extracted["filters"]["country"]
    elif context has country:
        country = context["country"]

    # Step 3: Validate time period exists
    has_time_period = bool(period or (start_date and end_date))

    # Step 4: Check what's missing
    missing_params = []
    if not seed_keywords: missing_params.append("seed_keywords")
    if not country: missing_params.append("country")
    if not has_time_period: missing_params.append("time_period")

    # Step 5: Ask for missing params
    if missing_params:
        needs_user_input = True
        clarification_prompt = create_intelligent_prompt(missing_params)
        return state

    # Step 6: All present - proceed
    trigger_endpoint()
```

---

## Testing Checklist

- ✅ Question with all 3 parameters → Triggers immediately
- ✅ Question missing 1 parameter → Asks for it
- ✅ Question missing 2 parameters → Asks for both
- ✅ Question missing all 3 → Asks for all
- ✅ General question → Asks for all 3 with helpful examples
- ✅ Uses UI context when available
- ✅ Question parameters override UI context
- ✅ Multi-turn conversation to collect missing params

---

## Benefits

1. **✅ Intelligent** - Only asks for what's missing
2. **✅ Helpful** - Provides examples in prompts
3. **✅ Flexible** - Works with UI context or question-based
4. **✅ Clear** - User always knows what's needed
5. **✅ Efficient** - Triggers endpoint as soon as possible
6. **✅ User-Friendly** - Natural conversation flow

---

## Next Steps

The Intent chat now validates parameters before executing. Try these test cases:

1. "Show me trending keywords" → Should ask for all 3 parameters
2. "Keywords for shoes in Canada" → Should ask for time period
3. "What are trending keywords for the last month?" → Should ask for keywords and country
4. "Insights for coffee in US for last 7 days" → Should trigger immediately

Enjoy the smart parameter validation! 🎉
