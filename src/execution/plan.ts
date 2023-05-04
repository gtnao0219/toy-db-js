import { BoundAssignment } from "../binder/bound";
import { Schema } from "../catalog/schema";
import { ExpressionAST, SelectElementAST, SortKeyAST } from "../parser/ast";

export type PlanNode =
  | SeqScanPlanNode
  | FilterPlanNode
  | ProjectPlanNode
  | SortPlanNode
  | LimitPlanNode
  | InsertPlanNode
  | DeletePlanNode
  | UpdatePlanNode;
export type SeqScanPlanNode = {
  type: "seq_scan";
  tableOid: number;
  schema: Schema;
};
export type FilterPlanNode = {
  type: "filter";
  condition: ExpressionAST;
  child: PlanNode;
};
export type ProjectPlanNode = {
  type: "project";
  selectElements: SelectElementAST[];
  child: PlanNode;
};
export type SortPlanNode = {
  type: "sort";
  sortKeys: SortKeyAST[];
  child: PlanNode;
};
export type LimitPlanNode = {
  type: "limit";
  count: number;
  child: PlanNode;
};
export type InsertPlanNode = {
  type: "insert";
  tableOid: number;
  schema: Schema;
  values: ExpressionAST[];
};
export type DeletePlanNode = {
  type: "delete";
  tableOid: number;
  schema: Schema;
  child: PlanNode;
};
export type UpdatePlanNode = {
  type: "update";
  tableOid: number;
  schema: Schema;
  assignments: BoundAssignment[];
  child: PlanNode;
};
