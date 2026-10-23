# Execution Evidence — Gen AI (personalized email)

Component: **Gen AI — make it intelligent.**
Endpoint: `databricks-meta-llama-3-3-70b-instruct` (Foundation Model API), called by the
app's email generation (`app.py` `generate_driver_email` / `generate_offer_email`).

## Prompt (system + user)
- **System:** "You write concise insurance emails."
- **User:** "You are a friendly insurance agent at Tripwise writing a short personalized
  retention email to driver Isabella Kim (device DEV1000), whose safe-driving score is 78
  and who qualifies for a 5.5% premium discount. Keep it under 120 words, warm and specific."

## Real model output
```
Subject: You're a Safe Driver, Isabella!

Hi Isabella,

Congratulations on a safe-driving score of 78! We're pleased to inform you that you
qualify for a 5.5% premium discount. Your responsible driving habits are paying off!
Keep up the good work with DEV1000. We appreciate having you as a valued customer at
Tripwise.

Best,
[Your Name]
Tripwise Insurance Agent
```

## How to reproduce
```bash
databricks serving-endpoints query databricks-meta-llama-3-3-70b-instruct -p fevm-target \
  --json '{"messages":[
    {"role":"system","content":"You write concise insurance emails."},
    {"role":"user","content":"<prompt above>"}]}'
```

> Driver/discount values are synthetic demo inputs.
