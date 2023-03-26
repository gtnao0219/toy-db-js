export type AST = CreateTableStmtAST | InsertStmtAST | SelectStmtAST;
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
export type SelectStmtAST = {
  type: "select_stmt";
  tableName: string;
  isAsterisk: boolean;
  columnNames: string[];
};
