import { Schema } from "../catalog/schema";
import {
  BinaryOperator,
  Direction,
  JoinType,
  UnaryOperator,
} from "../parser/ast";
import { LiteralValue } from "../parser/token";

// statement
export type BoundStatement =
  | BoundCreateTableStatement
  | BoundDropTableStatement
  | BoundSelectStatement
  | BoundInsertStatement
  | BoundUpdateStatement
  | BoundDeleteStatement;
export type BoundCreateTableStatement = {
  type: "create_table_statement";
  tableName: string;
  schema: Schema;
};
export type BoundDropTableStatement = {
  type: "drop_table_statement";
  tableReference: BoundBaseTableReference;
};
export type BoundSelectStatement = {
  type: "select_statement";
  isAsterisk: boolean;
  selectElements: BoundSelectElement[];
  tableReference: BoundTableReference;
  condition?: BoundExpression;
  groupBy?: BoundPathExpression[];
  having?: BoundExpression;
  orderBy?: BoundOrderBy;
  limit?: BoundLimit;
};
export type BoundSelectElement = {
  expression: BoundExpression;
  alias?: string;
};
export type BoundOrderBy = {
  sortKeys: BoundSortKey[];
};
export type BoundSortKey = {
  expression: BoundPathExpression;
  direction: Direction;
};
export type BoundLimit = {
  count: BoundExpression;
};
export type BoundInsertStatement = {
  type: "insert_statement";
  tableReference: BoundBaseTableReference;
  values: BoundExpression[];
};
export type BoundUpdateStatement = {
  type: "update_statement";
  tableReference: BoundBaseTableReference;
  assignments: BoundAssignment[];
  condition?: BoundExpression;
};
export type BoundAssignment = {
  target: BoundPathExpression;
  value: BoundExpression;
};
export type BoundDeleteStatement = {
  type: "delete_statement";
  tableReference: BoundBaseTableReference;
  condition?: BoundExpression;
};

// table reference
export type BoundTableReference =
  | BoundBaseTableReference
  | BoundJoinTableReference
  | BoundSubqueryTableReference;
export type BoundBaseTableReference = {
  type: "base_table_reference";
  tableName: string;
  alias?: string;
  tableOid: number;
  schema: Schema;
};
export type BoundJoinTableReference = {
  type: "join_table_reference";
  joinType: JoinType;
  left: BoundTableReference;
  right: BoundTableReference;
  condition: BoundExpression;
};
export type BoundSubqueryTableReference = {
  type: "subquery_table_reference";
  query: BoundSelectStatement;
  name: string;
};

// expression;
export type BoundExpression =
  | BoundBinaryOperationExpression
  | BoundUnaryOperationExpression
  | BoundFunctionCallExpression
  | BoundLiteralExpression
  | BoundPathExpression;
export type BoundBinaryOperationExpression = {
  type: "binary_operation";
  operator: BinaryOperator;
  left: BoundExpression;
  right: BoundExpression;
};
export type BoundUnaryOperationExpression = {
  type: "unary_operation";
  operator: UnaryOperator;
  operand: BoundExpression;
};
export type BoundFunctionCallExpression = {
  type: "function_call";
  functionName: string;
  args: BoundExpression[];
};
export type BoundPathExpression = {
  type: "path";
  path: string[];
};
export type BoundLiteralExpression = {
  type: "literal";
  value: LiteralValue;
};
