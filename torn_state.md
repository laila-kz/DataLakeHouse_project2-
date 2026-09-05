## Crash Recovery Test - Torn State

**Date:** 2026-07-29
**Simulated Crash:** After MERGE verification, before watermark advancement

**State Observed:**
| Component | Status |
|-----------|--------|
| Silver Table | Contains new test rows ✅ |
| Watermark | Still at previous value ❌ (not advanced) |
| Logs | "merge_verify_complete" present, "watermark_advance_complete" absent |

**Conclusion:** This is the exact "torn" state expected from Day 15's design. Data is written, but the watermark hasn't been updated to reflect it.