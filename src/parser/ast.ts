import { LiteralValue } from "./token";

export type StatementAST =
  | CreateTableStatementAST
  | DropTableStatementAST
  | SelectStatementAST
  | InsertStatementAST
  | UpdateStatementAST
  | DeleteStatementAST
  | BeginStatementAST
  | CommitStatementAST
  | RollbackStatementAST;
export type CreateTableStatementAST = {
  type: "create_table_statement";
  tableName: string;
  tableElements: TableElementAST[];
};
export type TableElementAST = {
  columnName: string;
  dataType: string;
};
export type DropTableStatementAST = {
  type: "drop_table_statement";
  tableName: string;
};
export type SelectStatementAST = {
  type: "select_statement";
  isAsterisk: boolean;
  selectElements: SelectElementAST[];
  // TODO: optional
  tableReference: TableReferenceAST;
  condition?: ExpressionAST;
  orderBy?: OrderByAST;
  limit?: LimitAST;
};
export type SelectElementAST = {
  expression: ExpressionAST;
  alias?: string;
};
export type TableReferenceAST =
  | BaseTableReferenceAST
  | JoinTableReferenceAST
  | SubqueryTableReferenceAST;
export type BaseTableReferenceAST = {
  type: "base_table_reference";
  tableName: string;
  alias?: string;
};
export type JoinType = "INNER" | "LEFT" | "RIGHT";
export type JoinTableReferenceAST = {
  type: "join_table_reference";
  joinType: JoinType;
  left: TableReferenceAST;
  right: TableReferenceAST;
  condition: ExpressionAST;
};
export type SubqueryTableReferenceAST = {
  type: "subquery_table_reference";
  query: SelectStatementAST;
  name: string;
};
export type OrderByAST = {
  sortKeys: SortKeyAST[];
};
export type Direction = "ASC" | "DESC";
export type SortKeyAST = {
  expression: ExpressionAST;
  direction: Direction;
};
export type LimitAST = {
  // TODO: support expression
  count: number;
};
export type InsertStatementAST = {
  type: "insert_statement";
  tableName: string;
  // TODO: support multiple rows and select statement
  values: ExpressionAST[];
};
export type DeleteStatementAST = {
  type: "delete_statement";
  tableName: string;
  condition?: ExpressionAST;
};
export type UpdateStatementAST = {
  type: "update_statement";
  tableName: string;
  assignments: AssignmentAST[];
  condition?: ExpressionAST;
};
export type AssignmentAST = {
  // TODO: support path expression
  columnName: string;
  value: ExpressionAST;
};
export type BeginStatementAST = {
  type: "begin_statement";
};
export type CommitStatementAST = {
  type: "commit_statement";
};
export type RollbackStatementAST = {
  type: "rollback_statement";
};
export type ExpressionAST =
  | BinaryOperationExpressionAST
  | UnaryOperationExpressionAST
  | LiteralExpressionAST
  | PathExpressionAST;
export type BinaryOperator =
  | "OR"
  | "AND"
  | "="
  | "<"
  | ">"
  | "<="
  | ">="
  | "<>"
  | "+"
  | "-"
  | "*";
export type BinaryOperationExpressionAST = {
  type: "binary_operation";
  operator: BinaryOperator;
  left: ExpressionAST;
  right: ExpressionAST;
};
export type UnaryOperator = "NOT";
export type UnaryOperationExpressionAST = {
  type: "unary_operation";
  operator: UnaryOperator;
  operand: ExpressionAST;
};
export type PathExpressionAST = {
  type: "path";
  path: string[];
};
export type LiteralExpressionAST = {
  type: "literal";
  value: LiteralValue;
};
