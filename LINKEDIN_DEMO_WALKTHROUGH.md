# Building a "Churn Prevention" AI Agent: A Walkthrough

This document outlines the step-by-step process to build a real-world "Churn Prevention" system using the SOP Data Admin Agent. This walkthrough demonstrates the "Manager-Worker" workflow where you direct the AI to author persistent, deterministic programs.

**Prerequisite:** Ensure your SOP Data Admin Agent is running and connected (typically via CLI or Web UI).

---

## Phase 1: Define the Micro-Logic ("The Function")

We start by creating a reusable, atomic script to assess a single customer's risk.

**1. Create the Script Shell**
Tell the agent to initialize the script.

```text
/create_script params=customer_id name="check_churn_risk" description="Analyzes a customer's order history to determine churn risk."
```

**2. Fetch Data (Authoring Step 1)**
We need to get the order history. We instruct the agent to append a `fetch` step.

```text
/save_step script="check_churn_risk" type="tool" name="fetch_orders" description="Fetch all orders for the given customer" command="select" args={"store": "orders", "key": "{{.customer_id}}"} result_var="orders_list"
```

**3. Define Logic (Authoring Step 2)**
We add a step to analyze the data. Since this requires date math (which is hard for simple DB operators), we use the `ask` step to let the LLM evaluate the JSON data deterministically.

```text
/save_step script="check_churn_risk" type="ask" name="evaluate_risk" description="Analyze orders to determine risk" prompt="Here is the order history: {{.orders_list}}. Today is {{now}}. A customer is HIGH RISK if they have 0 orders OR their last order was > 60 days ago AND total spend > $500. Return JSON: { \"is_risk\": bool, \"reason\": string }." output_variable="risk_analysis"
```

*Note: In a production environment, we would use `math` and `compare` steps for raw speed, but `ask` is powerful for complex business rules.*

**4. Return Result**
```text
/save_step script="check_churn_risk" type="command" description="Return the analysis" command="echo" args={"result": "{{.risk_analysis}}"}
```

---

## Phase 2: Interactive Debugging

Now that we have a "function", we test it immediately.

**1. Seed Test Data**
```text
/tool add store="orders" key="user_123" value=[{"id": 1, "date": "2024-01-01", "total": 600}]
```

**2. Run the Script**
```text
/play check_churn_risk customer_id="user_123"
```

**Expected Output:**
The Agent should load the script, fetch the order (dated 2024, definitely > 60 days ago), pass it to the "Brain" step, and output:
```json
{
  "is_risk": true,
  "reason": "Last order was over 60 days ago (2024-01-01) and total spend > $500."
}
```

---

## Phase 3: Assembly ("The Controller")

Now we build the "Main Program" that iterates over the database and uses our tool.

**1. Create Controller Script**
```text
/create_script name="daily_churn_scan" description="Scans all customers and emails at-risk ones."
```

**2. Scan Customers (Stream)**
```text
/save_step script="daily_churn_scan" type="tool" name="scan_users" command="select" args={"store": "customers"} result_var="customer_cursor"
```

**3. Map/Reduce Loop**
We append a loop that processes each customer.

```text
/save_step script="daily_churn_scan" type="loop" name="process_each" iterator="customer" list="customer_cursor" steps=[
  {
    "type": "script",
    "name": "check_risk",
    "script_name": "check_churn_risk",
    "script_args": {"customer_id": "{{.customer.id}}"},
    "result_var": "risk_result"
  },
  {
    "type": "if",
    "condition": {"is_risk": true},
    "input_var": "risk_result",
    "then": [
      {
        "type": "command",
        "command": "log",
        "args": {"message": "Sending coupon to {{.customer.email}} due to: {{.risk_result.reason}}"}
      }
    ]
  }
]
```

*Note: Writing nested JSON in a single CLI command is hard. In practice, the LLM constructs this for you when you say: "Loop over the cursor, call check_churn_risk, and if true, log it."*

---

## Phase 4: Final Verification

Run the full job.

```text
/play daily_churn_scan
```

This will:
1.  Open a detailed trace.
2.  Stream through every customer in the B-Tree.
3.  Execute the logic.
4.  Print the logs.

## Conclusion for Article

This walkthrough demonstrates the power of SOP:
1.  **Composable**: `check_churn_risk` is a standalone unit.
2.  **Hybrid**: We mixed Database ops (`fetch`) with GenAI ops (`ask`).
3.  **Managed**: The entire program is stored in `SystemDB` and can be versioned, audited, and scheduled.
