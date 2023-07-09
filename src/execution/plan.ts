import { Schema } from "../catalog/schema";
import { Direction, JoinType } from "../parser/ast";
import { ExpressionPlanNode, PathExpressionPlanNode } from "./expression_plan";

export type PlanNode =
  | SeqScanPlanNode
  | NestedLoopJoinPlanNode
  | FilterPlanNode
  | AggregatePlanNode
  | ProjectPlanNode
  | SortPlanNode
  | LimitPlanNode
  | InsertPlanNode
  | UpdatePlanNode
  | DeletePlanNode;
export type SeqScanPlanNode = {
  type: "seq_scan";
  tableOid: number;
  outputSchema: Schema;
};
export type NestedLoopJoinPlanNode = {
  type: "nested_loop_join";
  joinType: JoinType;
  condition: ExpressionPlanNode;
  left: PlanNode;
  right: PlanNode;
  outputSchema: Schema;
};
export type FilterPlanNode = {
  type: "filter";
  condition: ExpressionPlanNode;
  outputSchema: Schema;
  child: PlanNode;
};
export type AggregatePlanNode = {
  type: "aggregate";
  groupBy: PathExpressionPlanNode[];
  aggregations: ExpressionPlanNode[];
  aggregationTypes: AggregationType[];
  outputSchema: Schema;
  child: PlanNode;
};
export type AggregationType = "count" | "sum" | "max" | "min";
export type ProjectPlanNode = {
  type: "project";
  selectElements: SelectElementPlanNode[];
  outputSchema: Schema;
  child: PlanNode;
};
export type SelectElementPlanNode = {
  expression: ExpressionPlanNode;
  alias?: string;
};
export type SortPlanNode = {
  type: "sort";
  sortKeys: SortKeyPlanNode[];
  outputSchema: Schema;
  child: PlanNode;
};
export type SortKeyPlanNode = {
  expression: PathExpressionPlanNode;
  direction: Direction;
};
export type LimitPlanNode = {
  type: "limit";
  count: ExpressionPlanNode;
  outputSchema: Schema;
  child: PlanNode;
};
export type InsertPlanNode = {
  type: "insert";
  tableOid: number;
  values: ExpressionPlanNode[];
  outputSchema: Schema;
};
export type UpdatePlanNode = {
  type: "update";
  tableOid: number;
  assignments: AssignmentPlanNode[];
  outputSchema: Schema;
  child: PlanNode;
};
export type AssignmentPlanNode = {
  target: PathExpressionPlanNode;
  value: ExpressionPlanNode;
};
export type DeletePlanNode = {
  type: "delete";
  tableOid: number;
  outputSchema: Schema;
  child: PlanNode;
};
