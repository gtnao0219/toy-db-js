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
  type: "table_element";
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
  type: "assignment";
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
  columnNames: string[];
  condition?: ExpressionAST;
  orderBy?: OrderByAST;
  limit?: LimitAST;
};
export type OrderByAST = {
  type: "order_by";
  sortKeys: SortKeyAST[];
};
export type SortKeyAST = {
  type: "sort_key";
  columnName: string;
  direction: "ASC" | "DESC";
};
export type LimitAST = {
  type: "limit";
  value: number;
};
export type ExpressionAST = {
  type: string;
  value?: LiteralValue;
  left?: ExpressionAST;
  right?: ExpressionAST;
};
