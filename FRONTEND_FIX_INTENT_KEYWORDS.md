# Frontend Fix for Intent Chat - Use Module Seed Keywords

## Overview
The Intent chat should use the seed keywords and country selected in the Intent module UI, not extract them from the user's question. This allows users to ask general questions while the chat uses their pre-configured keywords.

---

## Fix 1: Pass Seed Keywords to AIChatComponent

### File: `IntentInsights.jsx`

Around line 534, where you render the `AIChatComponent`:

**BEFORE:**
```jsx
<AIChatComponent
  chatType="intent"
  selectedAccount={selectedAccount}  // Pass the whole object!
  period={period}
/>
```

**AFTER:**
```jsx
<AIChatComponent
  chatType="intent"
  selectedAccount={selectedAccount}  // Pass the whole object!
  selectedKeywords={seedKeywords}     // ← ADD THIS LINE
  selectedCountry={selectedCountry}   // ← ADD THIS LINE
  period={period}
/>
```

---

## Fix 2: Update AIChatComponent to Use Seed Keywords from Props

### File: `AIChatComponent.jsx`

### Step 1: Add props to component signature

Around line 4-12, add the new props:

**BEFORE:**
```javascript
const AIChatComponent = ({
  chatType,
  activeCampaign,
  activeProperty,
  selectedAccount,
  selectedCampaigns,
  selectedPage,
  period,
  customDates
}) => {
```

**AFTER:**
```javascript
const AIChatComponent = ({
  chatType,
  activeCampaign,
  activeProperty,
  selectedAccount,
  selectedKeywords,    // ← ADD THIS LINE
  selectedCountry,     // ← ADD THIS LINE
  selectedCampaigns,
  selectedPage,
  period,
  customDates
}) => {
```

### Step 2: Update buildContextData function

Around line 281-296, update the intent context building:

**BEFORE:**
```javascript
} else if (chatType === 'intent' && selectedAccount) {
  context.account_id = selectedAccount.customerId || selectedAccount.id;
  context.account_name = selectedAccount.name || selectedAccount.descriptiveName;
  // Add seed keywords if available from the intent module
  context.seed_keywords = selectedAccount.seed_keywords || [];
  context.country = selectedAccount.country || 'US';
}
```

**AFTER:**
```javascript
} else if (chatType === 'intent' && selectedAccount) {
  context.account_id = selectedAccount.customerId || selectedAccount.id;
  context.account_name = selectedAccount.name || selectedAccount.descriptiveName;
  // Use seed keywords from Intent module UI
  context.seed_keywords = selectedKeywords || [];
  // Use country from Intent module UI
  context.country = selectedCountry === "World Wide earth" ? "World Wide" : (selectedCountry || 'US');
}
```

---

## How It Works After the Fix

### Scenario 1: User has selected seed keywords in the UI
```
UI: seed_keywords = ["cosmetics", "beauty", "skincare"]
    country = "United States"

User asks: "What are the trending keywords in my industry?"

Backend uses:
✅ seed_keywords: ["cosmetics", "beauty", "skincare"]  (from UI)
✅ country: "United States"  (from UI)
✅ Calls keyword insights API with these parameters
```

### Scenario 2: User mentions specific keywords in the question
```
UI: seed_keywords = []  (empty)
    country = "US"

User asks: "Show me keyword insights for luxury watches"

Backend uses:
✅ seed_keywords: ["luxury watches"]  (extracted from question)
✅ country: "US"  (from UI)
✅ Calls keyword insights API with these parameters
```

### Scenario 3: Both UI keywords AND question keywords
```
UI: seed_keywords = ["shoes", "sneakers"]
    country = "Canada"

User asks: "Are there trends for boots?"

Backend uses:
✅ seed_keywords: ["shoes", "sneakers"]  (from UI - takes priority!)
✅ country: "Canada"  (from UI)
❌ Does NOT use "boots" from question (UI keywords take priority)
```

---

## Priority Order (Backend Logic)

The backend now follows this priority order:

1. **Seed Keywords:**
   - First: Use keywords from Intent module UI (if not empty)
   - Fallback: Extract from user question

2. **Country:**
   - First: Use country from Intent module UI (if set)
   - Fallback: Extract from user question
   - Default: "US"

3. **Ad Account:**
   - Always: Use selected account from Intent module UI

This ensures users can set their parameters in the UI and then ask natural language questions without having to repeat the keywords in every question.

---

## Testing

After making these changes:

1. Go to Intent Insights module
2. Select an ad account
3. Add seed keywords (e.g., "marketing", "advertising")
4. Select a country
5. Open the AI chat
6. Ask: "What are the trending keywords?" (without mentioning specific keywords)
7. ✅ Chat should use your UI-selected keywords automatically!

---

## Backend Changes (Already Applied)

✅ Fixed internal API caller to convert dict to Pydantic model
✅ Updated Agent 2 to prioritize context seed keywords over extracted ones
✅ Updated context preparation to use account_id correctly
✅ Updated API parameter building to use customer_id correctly
