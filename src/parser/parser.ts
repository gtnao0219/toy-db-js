import { TableElementAST } from "./ast";
import { InsertStmtAST } from "./ast";
import { SelectStmtAST } from "./ast";
import { CreateTableStmtAST } from "./ast";
import { AST } from "./ast";
import { tokenize } from "./lexer";
import { Keyword } from "./token";
import { Token } from "./token";

export class Parser {
  private tokens: Token[];
  constructor(input: string, private position: number = 0) {
    this.tokens = tokenize(input);
  }
  parse(): AST {
    return this.stmt();
  }
  private stmt(): AST {
    const token = this.tokens[this.position];
    if (token.type !== "keyword") {
      throw new Error(`Expected keyword but got ${token.type}`);
    }
    switch (token.value) {
      case "INSERT":
        return this.insertStmt();
      case "SELECT":
        return this.selectStmt();
      case "CREATE":
        return this.createTableStmt();
      default:
        throw new Error(`Unexpected keyword ${token.value}`);
    }
  }
  private createTableStmt(): CreateTableStmtAST {
    this.consumeKeyword("CREATE");
    this.consumeKeyword("TABLE");
    const tableName = this.consumeIdentifierOrError();
    this.consumeOrError("left_paren");
    const tableElements = [];
    tableElements.push(this.tableElement());
    while (this.consume("comma")) {
      tableElements.push(this.tableElement());
    }
    this.consumeOrError("right_paren");
    return {
      type: "create_table_stmt",
      tableName,
      tableElements,
    };
  }
  private tableElement(): TableElementAST {
    const columnName = this.consumeIdentifierOrError();
    let columnType: string;
    if (this.consumeKeyword("INTEGER")) {
      columnType = "INTEGER";
    } else if (this.consumeKeyword("VARCHAR")) {
      columnType = "VARCHAR";
    } else if (this.consumeKeyword("BOOLEAN")) {
      columnType = "BOOLEAN";
    } else {
      throw new Error("Expected column type");
    }
    return {
      type: "table_element",
      columnName,
      columnType: columnType,
    };
  }
  private insertStmt(): InsertStmtAST {
    this.consumeKeyword("INSERT");
    this.consumeKeyword("INTO");
    const tableName = this.consumeIdentifierOrError();
    this.consumeKeyword("VALUES");
    this.consumeOrError("left_paren");
    const values = [];
    values.push(this.consumeLiteralOrError());
    while (this.consume("comma")) {
      values.push(this.consumeLiteralOrError());
    }
    this.consumeOrError("right_paren");
    return {
      type: "insert_stmt",
      tableName,
      values,
    };
  }
  private selectStmt(): SelectStmtAST {
    this.consumeKeyword("SELECT");
    const isAsterisk = this.consume("asterisk");
    const columnNames = [];
    if (!isAsterisk) {
      columnNames.push(this.consumeIdentifierOrError());
      while (this.consume("comma")) {
        columnNames.push(this.consumeIdentifierOrError());
      }
    }
    this.consumeKeyword("FROM");
    const tableName = this.consumeIdentifierOrError();
    return {
      type: "select_stmt",
      tableName,
      isAsterisk,
      columnNames,
    };
  }
  private consume(tokenType: string): boolean {
    if (this.tokens[this.position].type === tokenType) {
      this.position++;
      return true;
    }
    return false;
  }
  private consumeOrError(tokenType: string): void {
    if (this.tokens[this.position].type === tokenType) {
      this.position++;
      return;
    }
    throw new Error(
      `Expected ${tokenType} but got ${this.tokens[this.position].type}`
    );
  }
  private consumeKeyword(keyword: Keyword): boolean {
    const token = this.tokens[this.position];
    if (token.type === "keyword" && token.value === keyword) {
      this.position++;
      return true;
    }
    return false;
  }
  private consumeKeywordOrError(keyword: Keyword): void {
    const token = this.tokens[this.position];
    if (token.type === "keyword" && token.value === keyword) {
      this.position++;
      return;
    }
    throw new Error(
      `Expected keyword but got ${this.tokens[this.position].type}`
    );
  }
  private consumeIdentifierOrError(): string {
    const token = this.tokens[this.position];
    if (token.type === "identifier") {
      const identifier = token.value;
      this.position++;
      return identifier;
    }
    throw new Error(
      `Expected identifier but got ${this.tokens[this.position].type}`
    );
  }
  private consumeLiteralOrError(): string | number | boolean {
    const token = this.tokens[this.position];
    if (token.type === "literal") {
      const value = token.value;
      this.position++;
      return value.value;
    }
    throw new Error(
      `Expected literal but got ${this.tokens[this.position].type}`
    );
  }
}
