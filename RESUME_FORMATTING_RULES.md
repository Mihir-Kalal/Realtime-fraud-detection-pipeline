# Resume Formatting Rules

Based on the feedback provided, here are the core principles for writing strong, senior-level resume bullet points:

1. **Contextualize Performance Metrics**
   - *Rule:* High numbers (like requests/sec) are meaningless without context. 
   - *Action:* Always specify the workload. If discussing a scoring/inference endpoint, state the payload size (e.g., "2KB JSON") or model complexity (e.g., "XGBoost tree depth").

2. **Lead with Systems Engineering Insights**
   - *Rule:* Specific techniques sound much more credible than generic "made it faster" claims.
   - *Action:* Name the real techniques used (e.g., "micro-batching + coalescing"). Lead with your strongest systems insight.

3. **Use Production-Scale Framing**
   - *Rule:* Avoid test-case numbers (e.g., "14/14") as they sound like homework or academic projects.
   - *Action:* Describe the failure-handling architecture itself (e.g., "Kafka DLQ architecture"), or replace with numbers that signal real volume (e.g., "failures/day", "% of stream volume").

4. **Highlight Advanced Patterns Early**
   - *Rule:* Senior-level patterns (like PSI-triggered retraining) establish immediate credibility.
   - *Action:* Move your most advanced MLOps or architectural patterns to the #1 or #2 bullet point. Don't bury your best work at the bottom.
