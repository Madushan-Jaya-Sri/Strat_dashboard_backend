# Frontend Fix for Intent Chat Account ID

## File to Update
`AIChatComponent.jsx`

## Location
Around line 228-230 in the `sendMessageToAPI` function

## Change Required

**BEFORE:**
```javascript
} else if (chatType === 'intent' && selectedAccount) {
  payload.customer_id = selectedAccount.customerId || selectedAccount.id;
  payload.context.account_id = selectedAccount.customerId || selectedAccount.id;
}
```

**AFTER:**
```javascript
} else if (chatType === 'intent' && selectedAccount) {
  payload.account_id = selectedAccount.customerId || selectedAccount.id;
  payload.customer_id = selectedAccount.customerId || selectedAccount.id;
  payload.context.account_id = selectedAccount.customerId || selectedAccount.id;
}
```

## Explanation
Add `payload.account_id` at the top level so the backend's `request.account_id` receives the value.

The backend code in `new_chat_manager.py:334` now correctly maps it:
```python
context["customer_id"] = request.account_id or request.customer_id or context.get("customer_id")
```

## After the Fix
The intent chat will automatically use the selected account without asking "Please select a Google Ads account first."
