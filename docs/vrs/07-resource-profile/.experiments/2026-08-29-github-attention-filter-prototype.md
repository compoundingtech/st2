# GitHub Resource attention-filter prototype

Date: 2026-08-29

## Question

How much notification noise can state-based filtering remove before delivery to an agent, and which policy choices remain semantic rather than mechanical?

## Method

A deterministic 16-observation sequence modeled one pull request moving through CI queue and execution states, duplicate provider observations, review activity, mergeability changes, a CI rerun, and merge. Four filters consumed the same sequence:

1. deliver every provider observation;
2. deliver each change to a facet's current state;
3. deliver changes classified as actionable, including CI success;
4. deliver only blocking or terminal changes.

The prototype counted candidate wakes. It did not call GitHub or an agent harness.

## Evidence

| Policy | Candidate wakes | Reduction | Delivered state classes |
| --- | ---: | ---: | --- |
| Raw observations | 16 | 0% | Every observation, including duplicates |
| State changes | 14 | 12.5% | Every distinct facet transition |
| Actionable changes | 7 | 56.25% | Review comment, CI failure/success, conflict, approval, merge |
| Blocking or terminal changes | 4 | 75% | CI failure, conflict, approval, merge |

Equal-state suppression removed only two duplicate observations. Most noise reduction came from semantic classification. The difference between seven and four wakes depended on whether CI success and a new review comment merit attention. A generic transport cannot infer that from byte changes alone.

## Result

State reconciliation and equal-state suppression are necessary but insufficient noise controls. A provider-aware layer must classify semantic changes before st2 applies generic coalescing, supersession, and delivery bounds. The experiment does not establish whether profile defaults alone are sufficient or whether each Resource binding needs an override.

## Conclusion

Provider-aware semantic classification is the load-bearing noise control. st2 should own only the mechanical coalescing, supersession, and delivery bounds that do not require provider semantics.

## Limits

The sequence is synthetic and intentionally small. It establishes mechanism boundaries, not production thresholds. Real GitHub event volume, agent wake behavior, and token cost remain unmeasured. CI systems with many parallel checks will amplify the difference between raw and semantic policies.

## VRS Impact

The Resource Profile design must separate provider-aware semantic classification from st2-owned mechanical delivery bounds. The interview must still decide whether the profile's default classification is final or whether a Resource binding can narrow or widen it.
