import { Schema } from "../catalog/schema";
import { Direction } from "../parser/ast";
import { ExpressionPlanNode, PathExpressionPlanNode } from "./expression_plan";

export type PlanNode =
  | SeqScanPlanNode
  | FilterPlanNode
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
export type FilterPlanNode = {
  type: "filter";
  condition: ExpressionPlanNode;
  outputSchema: Schema;
  child: PlanNode;
};
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
