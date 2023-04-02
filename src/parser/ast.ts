export type AST =
  | CreateTableStmtAST
  | InsertStmtAST
  | DeleteStmtAST
  | UpdateStmtAST
  | SelectStmtAST
  | BeginStmtAST
  | CommitStmtAST
  | RollbackStmtAST;
export type CreateTableStmtAST = {
  type: "create_table_stmt";
  tableName: string;
  tableElements: TableElementAST[];
};
export type TableElementAST = {
  type: "table_element";
  columnName: string;
  columnType: string;
};
export type InsertStmtAST = {
  type: "insert_stmt";
  tableName: string;
  values: Array<string | number | boolean>;
};
export type DeleteStmtAST = {
  type: "delete_stmt";
  tableName: string;
  condition?: ConditionAST;
};
export type BeginStmtAST = {
  type: "begin_stmt";
};
export type CommitStmtAST = {
  type: "commit_stmt";
};
export type RollbackStmtAST = {
  type: "rollback_stmt";
};
// TODO: support complex condition
export type ConditionAST = {
  type: "condition";
  columnName: string;
  right: string | number | boolean;
};
export type UpdateStmtAST = {
  type: "update_stmt";
  tableName: string;
  assignments: AssignmentAST[];
  condition?: ConditionAST;
};
export type AssignmentAST = {
  type: "assignment";
  columnName: string;
  value: string | number | boolean;
};
export type SelectStmtAST = {
  type: "select_stmt";
  tableName: string;
  isAsterisk: boolean;
  columnNames: string[];
  condition?: ConditionAST;
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
