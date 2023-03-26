export type Keyword =
  | "CREATE"
  | "TABLE"
  | "INSERT"
  | "INTO"
  | "VALUES"
  | "SELECT"
  | "FROM"
  | "INTEGER"
  | "VARCHAR"
  | "BOOLEAN";

export type Literal = StringLiteral | NumberLiteral | BooleanLiteral;
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

export type Token =
  | IdentifierToken
  | LiteralToken
  | AsteriskToken
  | SemicolonToken
  | CommaToken
  | LeftParenToken
  | RightParenToken
  | KeywordToken
  | EOFToken;

export type IdentifierToken = {
  type: "identifier";
  value: string;
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
export type LeftParenToken = {
  type: "left_paren";
};
export type RightParenToken = {
  type: "right_paren";
};
export type KeywordToken = {
  type: "keyword";
  value: Keyword;
};
export type EOFToken = {
  type: "eof";
};
