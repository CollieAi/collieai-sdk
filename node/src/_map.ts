/** Wire (snake_case) → SDK (camelCase) mapping helpers. */
import type { ContextModerationResult, TriggeredRule } from "./types.js";

export function toTriggeredRule(r: Record<string, unknown>): TriggeredRule {
  if (r === null || typeof r !== "object" || Array.isArray(r)) {
    // Match Pydantic: a primitive/list where a rule object is expected is a
    // malformed response, not a blank rule. Callers wrap this as invalid_response.
    throw new TypeError("triggered rule must be an object");
  }
  return {
    ruleId: (r.rule_id as string) ?? "",
    ruleName: (r.rule_name as string) ?? "",
    ruleType: (r.rule_type as string) ?? "",
    decision: (r.decision as string) ?? "",
    monitoring: Boolean(r.monitoring),
    matchInfo: (r.match_info as Record<string, unknown> | null) ?? null,
  };
}

/** Map the wire context_result → camelCase, or
 * null when the job carried no context analysis. */
export function toContextResult(c: unknown): ContextModerationResult | null {
  if (c === null || typeof c !== "object" || Array.isArray(c)) return null;
  const r = c as Record<string, unknown>;
  return {
    status: (r.status as string) ?? "not_provided",
    blocked: Boolean(r.blocked),
    blockMessage: (r.block_message as string | null) ?? null,
    triggeringPointer: (r.triggering_pointer as string | null) ?? null,
    triggeringRuleId: (r.triggering_rule_id as string | null) ?? null,
    triggeringRuleType: (r.triggering_rule_type as string | null) ?? null,
    parseDegraded: Boolean(r.parse_degraded),
    limitExceeded: Boolean(r.limit_exceeded),
    inferenceDegraded: Boolean(r.inference_degraded),
  };
}
