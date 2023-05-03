import { LiteralValue } from "./token";

export type StatementAST =
  | CreateTableStatementAST
  | DropTableStatementAST
  | InsertStatementAST
  | DeleteStatementAST
  | UpdateStatementAST
  | SelectStatementAST
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
  columnType: string;
};
export type DropTableStatementAST = {
  type: "drop_table_statement";
  tableName: string;
};
export type InsertStatementAST = {
  type: "insert_statement";
  tableName: string;
  columnNames?: string[];
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
export type SelectStatementAST = {
  type: "select_statement";
  isAsterisk: boolean;
  selectElements: SelectElementAST[];
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
  | SimpleTableReferenceAST
  | JoinTableReferenceAST
  | SubqueryTableReferenceAST;
export type SimpleTableReferenceAST = {
  type: "simple_table_reference";
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
  columnName: string;
  direction: Direction;
};
export type LimitAST = {
  value: number;
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
