import {
  AssignmentAST,
  BeginStatementAST,
  CommitStatementAST,
  CreateTableStatementAST,
  DeleteStatementAST,
  DropTableStatementAST,
  ExpressionAST,
  InsertStatementAST,
  LimitAST,
  OrderByAST,
  RollbackStatementAST,
  SelectStatementAST,
  SortKeyAST,
  StatementAST,
  TableElementAST,
  UpdateStatementAST,
} from "./ast";
import { tokenize } from "./lexer";
import { Keyword } from "./token";
import { Token } from "./token";

export class Parser {
  private tokens: Token[];
  private position: number = 0;
  constructor(input: string) {
    this.tokens = tokenize(input);
  }
  parse(): StatementAST {
    return this.statement();
  }
  private statement(): StatementAST {
    const token = this.tokens[this.position];
    if (token.type !== "keyword") {
      throw new Error(`Expected keyword but got ${token.type}`);
    }
    if (this.matchKeyword("CREATE")) {
      return this.createTableStatement();
    } else if (this.matchKeyword("DROP")) {
      return this.dropTableStatement();
    } else if (this.matchKeyword("INSERT")) {
      return this.insertStatement();
    } else if (this.matchKeyword("UPDATE")) {
      return this.updateStatement();
    } else if (this.matchKeyword("DELETE")) {
      return this.deleteStatement();
    } else if (this.matchKeyword("SELECT")) {
      return this.selectStatement();
    } else if (this.matchKeyword("BEGIN")) {
      return this.beginStatement();
    } else if (this.matchKeyword("COMMIT")) {
      return this.commitStatement();
    } else if (this.matchKeyword("ROLLBACK")) {
      return this.rollbackStatement();
    }
    throw new Error(`Unexpected keyword ${token.value}`);
  }
  private createTableStatement(): CreateTableStatementAST {
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
      type: "create_table_statement",
      tableName,
      tableElements,
    };
  }
  private tableElement(): TableElementAST {
    const columnName = this.consumeIdentifierOrError();
    let columnType: string;
    if (this.consumeKeyword("INTEGER")) {
      columnType = "INTEGER";
    } else if (this.consumeKeyword("FLOAT")) {
      columnType = "FLOAT";
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
  private dropTableStatement(): DropTableStatementAST {
    this.consumeKeywordOrError("DROP");
    this.consumeKeywordOrError("TABLE");
    const tableName = this.consumeIdentifierOrError();
    return {
      type: "drop_table_statement",
      tableName,
    };
  }
  private insertStatement(): InsertStatementAST {
    this.consumeKeywordOrError("INSERT");
    this.consumeKeywordOrError("INTO");
    const tableName = this.consumeIdentifierOrError();
    this.consumeKeywordOrError("VALUES");
    this.consumeOrError("left_paren");
    const values = [];
    values.push(this.expression());
    while (this.consume("comma")) {
      values.push(this.expression());
    }
    return {
      type: "insert_statement",
      tableName,
      values,
    };
  }
  private updateStatement(): UpdateStatementAST {
    this.consumeKeywordOrError("UPDATE");
    const tableName = this.consumeIdentifierOrError();
    this.consumeKeywordOrError("SET");
    const assignments = [];
    assignments.push(this.assignment());
    while (this.consume("comma")) {
      assignments.push(this.assignment());
    }
    let condition: ExpressionAST | undefined = undefined;
    if (this.consumeKeyword("WHERE")) {
      condition = this.expression();
    }
    return {
      type: "update_statement",
      tableName,
      assignments,
      condition,
    };
  }
  private assignment(): AssignmentAST {
    const columnName = this.consumeIdentifierOrError();
    this.consumeOrError("equal");
    const value = this.expression();
    return {
      type: "assignment",
      columnName,
      value,
    };
  }
  private deleteStatement(): DeleteStatementAST {
    this.consumeKeywordOrError("DELETE");
    this.consumeKeywordOrError("FROM");
    const tableName = this.consumeIdentifierOrError();
    let condition: ExpressionAST | undefined = undefined;
    if (this.consumeKeyword("WHERE")) {
      condition = this.expression();
    }
    return {
      type: "delete_statement",
      tableName,
      condition,
    };
  }
  private selectStatement(): SelectStatementAST {
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
    let condition: ExpressionAST | undefined = undefined;
    if (this.consumeKeyword("WHERE")) {
      condition = this.expression();
    }
    const orderBy = this.orderBy() ?? undefined;
    const limit = this.limit() ?? undefined;
    return {
      type: "select_statement",
      tableName,
      isAsterisk,
      columnNames,
      condition,
      orderBy,
      limit,
    };
  }

  private expression(): ExpressionAST {
    return this.logicalOr();
  }
  private logicalOr(): ExpressionAST {
    let left = this.logicalAnd();
    while (this.consumeKeyword("OR")) {
      const right = this.logicalAnd();
      left = {
        type: "logical_or",
        left,
        right,
      };
    }
    return left;
  }
  private logicalAnd(): ExpressionAST {
    let left = this.logicalNot();
    while (this.consumeKeyword("AND")) {
      const right = this.logicalNot();
      left = {
        type: "logical_and",
        left,
        right,
      };
    }
    return left;
  }
  private logicalNot(): ExpressionAST {
    if (this.consumeKeyword("NOT")) {
      const expr = this.comparison();
      return {
        type: "logical_not",
        left: expr,
      };
    }
    return this.comparison();
  }
  private comparison(): ExpressionAST {
    const left = this.arithmetic();
    if (this.consume("less_than")) {
      const right = this.arithmetic();
      return {
        type: "less_than",
        left,
        right,
      };
    }
    if (this.consume("greater_than")) {
      const right = this.arithmetic();
      return {
        type: "greater_than",
        left,
        right,
      };
    }
    if (this.consume("less_than_equal")) {
      const right = this.arithmetic();
      return {
        type: "less_than_or_equal",
        left,
        right,
      };
    }
    if (this.consume("greater_than_equal")) {
      const right = this.arithmetic();
      return {
        type: "greater_than_or_equal",
        left,
        right,
      };
    }
    if (this.consume("equal")) {
      const right = this.arithmetic();
      return {
        type: "equal",
        left,
        right,
      };
    }
    if (this.consume("not_equal")) {
      const right = this.arithmetic();
      return {
        type: "not_equal",
        left,
        right,
      };
    }
    return left;
  }
  private arithmetic(): ExpressionAST {
    let left = this.term();
    while (this.match("plus") || this.match("minus")) {
      const operator = this.consume("plus")
        ? "plus"
        : (this.consume("minus"), "minus");
      const right = this.term();
      left = {
        type: operator,
        left,
        right,
      };
    }
    return left;
  }
  private term(): ExpressionAST {
    let left = this.factor();
    while (this.match("asterisk")) {
      this.consume("asterisk");
      const right = this.factor();
      left = {
        type: "multiple",
        left,
        right,
      };
    }
    return left;
  }
  private factor(): ExpressionAST {
    if (this.match("literal")) {
      const value = this.consumeLiteralOrError();
      return {
        type: "literal",
        value,
      };
    }
    if (this.match("identifier")) {
      const columnName = this.consumeIdentifierOrError();
      return {
        type: "identifier",
        value: columnName,
      };
    }
    this.consumeOrError("left_paren");
    const expr = this.expression();
    this.consumeOrError("right_paren");
    return expr;
  }

  private orderBy(): OrderByAST | null {
    if (!(this.consumeKeyword("ORDER") && this.consumeKeyword("BY"))) {
      return null;
    }
    const sortKeys = [];
    sortKeys.push(this.sortKey());
    while (this.consume("comma")) {
      sortKeys.push(this.sortKey());
    }
    return {
      type: "order_by",
      sortKeys,
    };
  }
  private sortKey(): SortKeyAST {
    const columnName = this.consumeIdentifierOrError();
    let direction: "ASC" | "DESC" = "ASC";
    if (this.consumeKeyword("ASC")) {
      direction = "ASC";
    } else if (this.consumeKeyword("DESC")) {
      direction = "DESC";
    }
    return {
      type: "sort_key",
      columnName,
      direction,
    };
  }
  private limit(): LimitAST | null {
    if (!this.consumeKeyword("LIMIT")) {
      return null;
    }
    const value = this.consumeLiteralOrError();
    if (typeof value !== "number") {
      throw new Error("Expected number");
    }
    return {
      type: "limit",
      value,
    };
  }
  private beginStatement(): BeginStatementAST {
    this.consumeKeywordOrError("BEGIN");
    return {
      type: "begin_statement",
    };
  }
  private commitStatement(): CommitStatementAST {
    this.consumeKeywordOrError("COMMIT");
    return {
      type: "commit_statement",
    };
  }
  private rollbackStatement(): RollbackStatementAST {
    this.consumeKeywordOrError("ROLLBACK");
    return {
      type: "rollback_statement",
    };
  }
  private match(tokenType: string): boolean {
    return this.tokens[this.position].type === tokenType;
  }
  private matchKeyword(keyword: string): boolean {
    const current = this.tokens[this.position];
    return current.type === "keyword" && current.value === keyword;
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
  private consumeLiteralOrError(): string | number | boolean | null {
    const token = this.tokens[this.position];
    if (token.type === "literal") {
      const value = token.value;
      this.position++;
      if (value.type === "null") {
        return null;
      }
      return value.value;
    }
    throw new Error(
      `Expected literal but got ${this.tokens[this.position].type}`
    );
  }
}
