import {
  AssignmentAST,
  BeginStatementAST,
  CommitStatementAST,
  CreateTableStatementAST,
  DeleteStatementAST,
  DropTableStatementAST,
  ExpressionAST,
  InsertStatementAST,
  JoinTableReferenceAST,
  JoinType,
  LimitAST,
  OrderByAST,
  RollbackStatementAST,
  SelectElementAST,
  SelectStatementAST,
  SortKeyAST,
  StatementAST,
  TableElementAST,
  TableReferenceAST,
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
  private selectStatement(): SelectStatementAST {
    this.consumeKeywordOrError("SELECT");
    const isAsterisk = this.consume("asterisk");
    const selectElements = [];
    if (!isAsterisk) {
      selectElements.push(this.selectElement());
      while (this.consume("comma")) {
        selectElements.push(this.selectElement());
      }
    }
    this.consumeKeywordOrError("FROM");
    const tableReference = this.tableReference();
    let condition: ExpressionAST | undefined = undefined;
    if (this.consumeKeyword("WHERE")) {
      condition = this.expression();
    }
    const orderBy = this.orderBy() ?? undefined;
    const limit = this.limit() ?? undefined;
    return {
      type: "select_statement",
      isAsterisk,
      selectElements,
      tableReference,
      ...(condition != null ? { condition } : {}),
      ...(orderBy != null ? { orderBy } : {}),
      ...(limit != null ? { limit } : {}),
    };
  }
  private selectElement(): SelectElementAST {
    const expression = this.expression();
    if (this.consumeKeyword("AS")) {
      const alias = this.consumeIdentifierOrError();
      return {
        expression,
        alias,
      };
    }
    return {
      expression,
    };
  }
  private tableReference(): TableReferenceAST {
    let left: TableReferenceAST;

    if (this.consume("left_paren")) {
      const query = this.selectStatement();
      this.consumeOrError("right_paren");
      this.consumeKeywordOrError("AS");
      const name = this.consumeIdentifierOrError();
      left = {
        type: "subquery_table_reference",
        query,
        name,
      };
    } else {
      const tableName = this.consumeIdentifierOrError();
      left = {
        type: "simple_table_reference",
        tableName,
      };
      if (this.consumeKeyword("AS")) {
        left.alias = this.consumeIdentifierOrError();
      }
    }

    while (
      this.matchKeyword("INNER") ||
      this.matchKeyword("LEFT") ||
      this.matchKeyword("RIGHT")
    ) {
      left = this.joinTableReference(left);
    }

    return left;
  }
  private joinTableReference(left: TableReferenceAST): JoinTableReferenceAST {
    let joinType: JoinType;

    if (this.consumeKeyword("INNER")) {
      joinType = "INNER";
    } else if (this.consumeKeyword("LEFT")) {
      joinType = "LEFT";
    } else if (this.consumeKeyword("RIGHT")) {
      joinType = "RIGHT";
    } else {
      throw new Error("Unexpected join type");
    }

    this.consumeKeywordOrError("JOIN");
    const right = this.tableReference();
    this.consumeKeywordOrError("ON");
    const condition = this.expression();

    return {
      type: "join_table_reference",
      joinType,
      left,
      right,
      condition,
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

  // expressions
  private expression(): ExpressionAST {
    return this.logicalOr();
  }
  private logicalOr(): ExpressionAST {
    let left = this.logicalAnd();
    while (this.consumeKeyword("OR")) {
      const right = this.logicalAnd();
      left = {
        type: "binary_operation",
        operator: "OR",
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
        type: "binary_operation",
        operator: "AND",
        left,
        right,
      };
    }
    return left;
  }
  private logicalNot(): ExpressionAST {
    if (this.consumeKeyword("NOT")) {
      const operand = this.comparison();
      return {
        type: "unary_operation",
        operator: "NOT",
        operand,
      };
    }
    return this.comparison();
  }
  private comparison(): ExpressionAST {
    const left = this.arithmetic();
    if (this.consume("less_than")) {
      const right = this.arithmetic();
      return {
        type: "binary_operation",
        operator: "<",
        left,
        right,
      };
    }
    if (this.consume("greater_than")) {
      const right = this.arithmetic();
      return {
        type: "binary_operation",
        operator: ">",
        left,
        right,
      };
    }
    if (this.consume("less_than_equal")) {
      const right = this.arithmetic();
      return {
        type: "binary_operation",
        operator: "<=",
        left,
        right,
      };
    }
    if (this.consume("greater_than_equal")) {
      const right = this.arithmetic();
      return {
        type: "binary_operation",
        operator: ">=",
        left,
        right,
      };
    }
    if (this.consume("equal")) {
      const right = this.arithmetic();
      return {
        type: "binary_operation",
        operator: "=",
        left,
        right,
      };
    }
    if (this.consume("not_equal")) {
      const right = this.arithmetic();
      return {
        type: "binary_operation",
        operator: "<>",
        left,
        right,
      };
    }
    return left;
  }
  private arithmetic(): ExpressionAST {
    let left = this.term();
    while (this.match("plus") || this.match("minus")) {
      const isPlus = this.consume("plus");
      if (!isPlus) {
        this.consumeOrError("minus");
      }
      const right = this.term();
      left = {
        type: "binary_operation",
        operator: isPlus ? "+" : "-",
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
        type: "binary_operation",
        operator: "*",
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
      const path = [];
      path.push(this.consumeIdentifierOrError());
      while (this.consume("dot")) {
        path.push(this.consumeIdentifierOrError());
      }
      return {
        type: "path",
        path,
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
      sortKeys,
    };
  }
  private sortKey(): SortKeyAST {
    const expression = this.expression();
    let direction: "ASC" | "DESC" = "ASC";
    if (this.consumeKeyword("ASC")) {
      direction = "ASC";
    } else if (this.consumeKeyword("DESC")) {
      direction = "DESC";
    }
    return {
      expression,
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
