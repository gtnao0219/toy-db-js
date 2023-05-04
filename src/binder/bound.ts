import { Schema } from "../catalog/schema";
import {
  BinaryOperator,
  ExpressionAST,
  JoinType,
  LimitAST,
  OrderByAST,
  SelectElementAST,
  UnaryOperator,
} from "../parser/ast";
import { LiteralValue } from "../parser/token";

// statement
export type BoundStatement =
  | BoundCreateTableStatement
  | BoundDropTableStatement
  | BoundInsertStatement
  | BoundDeleteStatement
  | BoundUpdateStatement
  | BoundSelectStatement;
export type BoundCreateTableStatement = {
  type: "create_table_statement";
  tableName: string;
  schema: Schema;
};
export type BoundDropTableStatement = {
  type: "drop_table_statement";
  tableOid: number;
};
export type BoundInsertStatement = {
  type: "insert_statement";
  tableOid: number;
  schema: Schema;
  values: ExpressionAST[];
};
export type BoundDeleteStatement = {
  type: "delete_statement";
  tableOid: number;
  schema: Schema;
  condition?: ExpressionAST;
};
export type BoundUpdateStatement = {
  type: "update_statement";
  tableOid: number;
  schema: Schema;
  assignments: BoundAssignment[];
  condition?: ExpressionAST;
};
export type BoundAssignment = {
  columnIndex: number;
  value: ExpressionAST;
};
export type BoundSelectStatement = {
  type: "select_statement";
  isAsterisk: boolean;
  selectElements: BoundSelectElement[];
  tableReference: BoundTableReference;
  condition?: ExpressionAST;
  orderBy?: OrderByAST;
  limit?: LimitAST;
};
export type BoundSelectElement = {
  expression: BoundExpression;
  alias?: string;
};

// table reference
export type BoundTableReference =
  | BoundSimpleTableReference
  | BoundJoinTableReference
  | BoundSubqueryTableReference;
export type BoundSimpleTableReference = {
  type: "simple_table_reference";
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
  condition: ExpressionAST;
};
export type BoundSubqueryTableReference = {
  type: "subquery_table_reference";
  query: BoundSelectStatement;
  name: string;
};

// expression;
export type BoundExpression =
  | BoundBinaryOperatorExpression
  | BoundUnaryOperatorExpression
  | BoundLiteralExpression
  | BoundPathExpression;
export type BoundBinaryOperatorExpression = {
  type: "binary_operator_expression";
  operator: BinaryOperator;
  left: BoundExpression;
  right: BoundExpression;
};
export type BoundUnaryOperatorExpression = {
  type: "unary_operator_expression";
  operator: UnaryOperator;
  operand: BoundExpression;
};
export type BoundPathExpression = {
  type: "path_expression";
  path: string[];
};
export type BoundLiteralExpression = {
  type: "literal_expression";
  value: LiteralValue;
};

export function evaluate(expression: ExpressionAST): LiteralValue {}
