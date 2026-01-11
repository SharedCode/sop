# Programming with SOP: The "Manager-Worker" Workflow

In the traditional software development lifecycle, "programming" is a translation task. You have an idea in your head ("I need to email all users who haven't logged in for a month"), and you spend hours keying in syntax to explain that idea to a machine.

SOP changes this paradigm. In our platform, you don't write the code—you **manage** the entity that writes the code. You act as the Architect, and the SOP AI Agent acts as your tireless Staff Engineer.

Here is what it looks like to build a real, robust data application using "Natural Language Programming" in SOP.

## The Goal: A "Churn Prevention" Program

Let's say you want to build a system that identifies customers who are at risk of leaving and automatically sends them a discount coupon.

In a normal stack, this is a week of work: API endpoints, SQL queries, Cron jobs, email service integration.
In SOP, it is a 5-minute conversation.

### Phase 1: Defining the Protocol ("The Function")

First, you need to define *how* to calculate risk. You don't write a SQL query; you just explain the rule to the Agent.

**User:**
> "I want to create a script called `check_churn_risk`. It should take a `customer_id`.
> A customer is 'high risk' if they haven't ordered in 60 days AND their total spend is over $500.
> Start a draft."

**Agent (The Worker):**
> "I've created the draft `check_churn_risk`. I've added logical steps to fetch the user's order history and calculate the last order date.
> Should I return just a boolean, or the risk score?"

**User:**
> "Return a JSON object with `{ is_risk: bool, reason: string }`.
> Also, make sure to handle the case where they have NO orders."

**Agent:**
> "Understood. I've appended the logic to handle `null` order history as 'high risk' (dormant account). I've saved the script."

*Behind the scenes, the Agent authored a precise, deterministic JSON script with `fetch`, `if`, and `math` steps. It is now a compiled function in the system.*

### Phase 2: Unit Testing (Interactive Debugging)

Now you verify the logic. You don't write a unit test file; you just ask the Agent to dry-run it.

**User:**
> "Run `check_churn_risk` for customer 'john_doe'. Show me the trace."

**Agent:**
> "**Result:** `{ is_risk: true, reason: 'Last order was 94 days ago' }`
> **Trace:**
> 1. Fetched orders (Found 12)
> 2. Calculated days_since_last (94 > 60)
> 3. Checked total_spend ($1200 > $500)
> 4. Logic implies RISK."

**User:**
> "Perfect."

### Phase 3: Assembly (The "Main" Program)

Now that you have your Atomic Building Block (`check_churn_risk`), you can compose a larger workflow. You want to apply this to your entire database.

**User:**
> "Create a new process called `weekly_churn_prevention`.
> It should:
> 1. Scan all users in the 'customers' database.
> 2. For each user, run our `check_churn_risk` script.
> 3. If they are at risk, generate a secure coupon code.
> 4. Email them the coupon."

**Agent:**
> "I am assembling the loop.
> Since we might have 1,000,000 customers, I will use a **Map/Reduce** pattern.
> - **Map**: Stream all customer IDs.
> - **Process**: Call `check_churn_risk` and `email_service`.
> 
> I notice I don't have an `email_service` tool configured. Shall I mock it for now?"

**User:**
> "Yes, mock it. And save this as a Batch Job."

### Phase 4: Deployment

You have authored a complex data pipeline involving database scans, sub-routines (functions), conditional logic, and external side effects. You didn't write a single curly brace.

**User:**
> "Schedule `weekly_churn_prevention` to run every Monday at 9 AM."

**Agent:**
> "Scheduled. The system will wake up, mount the database, stream the users through your logic, and execute the actions. I will notify you of any failures."

## Why This Works

This workflow succeeds where "Code Gen" fails because of **Structure**.

1.  **Atomic Units**: You didn't try to generate the whole app in one prompt. You built a "Function" (`check_churn_risk`), verified it, and then *used* it.
2.  **Determinism**: Once `check_churn_risk` is saved, it is **frozen**. It is not an LLM hallucinating a new query every time; it is a rigid execution plan. The reliability is 100%.
3.  **Composable**: The "Main Program" doesn't need to know *how* churn is calculated. It just calls the script. If you change the definition of "Risk" later, you update one script, and the whole system updates.

## The Future of Programming

In SOP, "Programming" looks less like typing and more like **Product Management**.

*   You define the **Requirements**.
*   You review the **Implementation** (Step descriptions, names).
*   You conduct **User Acceptance Testing** (Running with sample inputs).
*   You **Sign Off** (Save script).

This is the fastest way to turn intent into reliable software.
