---
layout: global
title: Spark Improvement Proposals (SIP)
---

## Why?

The purpose of an SIP is to involve the community in major improvements to the Spark codebase before they happen, to increase the likelihood that user needs are met.

## When?

SIPs should be used for significant user-facing or cross-cutting changes, not day-to-day improvements.  When in doubt, if a committer thinks a change needs an SIP, it does.

## What?

An SIP is organized in a specially tagged Jira ticket and accompanying discussion.

## Where?

You can participate in [current SIPs](http://SOME-LINK-TO-A-JIRA-FILTER) or see [approved SIPs](http://SOME-LINK-TO-A-JIRA-FILTER-FOR-COMPLETED).

## Who?

* Users can help by discussing whether an SIP is likely to meet their needs, and by suggesting topics for future SIPs.
* Contributors can help by discussing whether an SIP is likely to be technically feasible.
* Committers can help by discussing whether an SIP aligns with long-term project goals, and by formally proposing and accepting SIPs.

## How?

### Template for an SIP
* Background: What problem is this solving?
* Goals: What must this allow people to do, that they can't currently?
* Rejected Goals: What shouldn't this allow?
* Strategy: How are the goals going to be accomplished? Give sufficient technical detail to allow a contributor to judge whether it's likely to be feasible. This is not a full design document. If it's discussing specifics of new internal classes or methods, it's likely too detailed.
* Rejected Strategies: What alternatives were considered? Why were they rejected?  If no alternatives have been considered, the problem needs more thought.

### Discussion of an SIP
All discussion of an SIP must take place in a public forum, preferably the discussion attached to the Jira.

### Acceptance of an SIP
In order to provide a clear outcome, once a committer believes discussion has run its course, s/he can call for a vote by [majority approval](https://www.apache.org/foundation/glossary.html#MajorityApproval). Changing goals or rejected goals after an SIP has been approved requires another vote. Changing specific implementation details after approval does not require another vote, provided that goals are met.

### Implementation of an SIP
Implementation should take place after acceptance, via the [standard process for code changes](https://cwiki.apache.org/confluence/display/SPARK/Contributing+to+Spark#ContributingtoSpark-PreparingtoContributeCodeChanges)