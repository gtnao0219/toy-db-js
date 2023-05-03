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
  // TODO: join
  tableName: string;
  isAsterisk: boolean;
  selectElements: SelectElementAST[];
  condition?: ExpressionAST;
  orderBy?: OrderByAST;
  limit?: LimitAST;
};
export type SelectElementAST = {
  expression: ExpressionAST;
  alias?: string;
};
export type OrderByAST = {
  sortKeys: SortKeyAST[];
};
export type SortKeyAST = {
  columnName: string;
  direction: "ASC" | "DESC";
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
