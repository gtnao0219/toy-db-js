export const KEYWORDS = [
  "CREATE",
  "TABLE",
  "INDEX",
  "DROP",
  "INSERT",
  "INTO",
  "VALUES",
  "DELETE",
  "FROM",
  "UPDATE",
  "SET",
  "SELECT",
  "INNER",
  "LEFT",
  "RIGHT",
  "JOIN",
  "ON",
  "WHERE",
  "GROUP",
  "BY",
  "HAVING",
  "ORDER",
  "DESC",
  "ASC",
  "LIMIT",
  "INTEGER",
  "VARCHAR",
  "BOOLEAN",
  "LIKE",
  "BETWEEN",
  "IN",
  "AND",
  "OR",
  "NOT",
  "AS",
  "BEGIN",
  "COMMIT",
  "ROLLBACK",
] as const;
export type Keyword = typeof KEYWORDS[number];

export type Literal =
  | StringLiteral
  | NumberLiteral
  | BooleanLiteral
  | NullLiteral;
export type StringLiteral = {
  type: "string";
  value: string;
};
export type NumberLiteral = {
  type: "number";
  value: number;
};
export type BooleanLiteral = {
  type: "boolean";
  value: boolean;
};
export type NullLiteral = {
  type: "null";
  value: null;
};
export type LiteralValue = Literal["value"];

export type Token =
  | IdentifierToken
  | KeywordToken
  | LiteralToken
  | AsteriskToken
  | SemicolonToken
  | CommaToken
  | DotToken
  | LeftParenToken
  | RightParenToken
  | EqualToken
  | NotEqualToken
  | LessThanToken
  | LessThanEqualToken
  | GreaterThanToken
  | GreaterThanEqualToken
  | PlusToken
  | MinusToken
  | EOFToken;

export type IdentifierToken = {
  type: "identifier";
  value: string;
};
export type KeywordToken = {
  type: "keyword";
  value: Keyword;
};
export type LiteralToken = {
  type: "literal";
  value: Literal;
};
export type AsteriskToken = {
  type: "asterisk";
};
export type SemicolonToken = {
  type: "semicolon";
};
export type CommaToken = {
  type: "comma";
};
export type DotToken = {
  type: "dot";
};
export type LeftParenToken = {
  type: "left_paren";
};
export type RightParenToken = {
  type: "right_paren";
};
export type EqualToken = {
  type: "equal";
};
export type NotEqualToken = {
  type: "not_equal";
};
export type GreaterThanToken = {
  type: "greater_than";
};
export type GreaterThanEqualToken = {
  type: "greater_than_equal";
};
export type LessThanToken = {
  type: "less_than";
};
export type LessThanEqualToken = {
  type: "less_than_equal";
};
export type PlusToken = {
  type: "plus";
};
export type MinusToken = {
  type: "minus";
};
export type EOFToken = {
  type: "eof";
};
