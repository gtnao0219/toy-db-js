import {
  AssignmentAST,
  ConditionAST,
  DeleteStmtAST,
  TableElementAST,
  UpdateStmtAST,
} from "./ast";
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
      case "CREATE":
        return this.createTableStmt();
      case "INSERT":
        return this.insertStmt();
      case "DELETE":
        return this.deleteStmt();
      case "UPDATE":
        return this.updateStmt();
      case "SELECT":
        return this.selectStmt();
      default:
        throw new Error(`Unexpected keyword ${token.value}`);
    }
  }
  private createTableStmt(): CreateTableStmtAST {
    this.consumeKeywordOrError("CREATE");
    this.consumeKeywordOrError("TABLE");
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
    this.consumeKeywordOrError("INSERT");
    this.consumeKeywordOrError("INTO");
    const tableName = this.consumeIdentifierOrError();
    this.consumeKeywordOrError("VALUES");
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
  private deleteStmt(): DeleteStmtAST {
    this.consumeKeywordOrError("DELETE");
    this.consumeKeywordOrError("FROM");
    const tableName = this.consumeIdentifierOrError();
    let condition: ConditionAST | undefined = undefined;
    if (this.consumeKeyword("WHERE")) {
      condition = this.condition();
    }
    return {
      type: "delete_stmt",
      tableName,
      condition,
    };
  }
  private updateStmt(): UpdateStmtAST {
    this.consumeKeywordOrError("UPDATE");
    const tableName = this.consumeIdentifierOrError();
    this.consumeKeywordOrError("SET");
    const assignments = this.assignments();
    let condition: ConditionAST | undefined = undefined;
    if (this.consumeKeyword("WHERE")) {
      condition = this.condition();
    }
    return {
      type: "update_stmt",
      tableName,
      assignments,
      condition,
    };
  }
  private assignments(): AssignmentAST[] {
    const res: AssignmentAST[] = [];
    const columnName = this.consumeIdentifierOrError();
    this.consumeOrError("equal");
    const value = this.consumeLiteralOrError();
    res.push({
      type: "assignment",
      columnName,
      value,
    });
    while (this.consume("comma")) {
      const columnName = this.consumeIdentifierOrError();
      this.consumeOrError("equals");
      const value = this.consumeLiteralOrError();
      res.push({
        type: "assignment",
        columnName,
        value,
      });
    }
    return res;
  }
  private selectStmt(): SelectStmtAST {
    this.consumeKeywordOrError("SELECT");
    const isAsterisk = this.consume("asterisk");
    const columnNames = [];
    if (!isAsterisk) {
      columnNames.push(this.consumeIdentifierOrError());
      while (this.consume("comma")) {
        columnNames.push(this.consumeIdentifierOrError());
      }
    }
    this.consumeKeywordOrError("FROM");
    const tableName = this.consumeIdentifierOrError();
    let condition: ConditionAST | undefined = undefined;
    if (this.consumeKeyword("WHERE")) {
      condition = this.condition();
    }
    return {
      type: "select_stmt",
      tableName,
      isAsterisk,
      columnNames,
      condition,
    };
  }
  private condition(): ConditionAST {
    const columnName = this.consumeIdentifierOrError();
    this.consumeOrError("equal");
    const right = this.consumeLiteralOrError();
    return {
      type: "condition",
      columnName,
      right,
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
