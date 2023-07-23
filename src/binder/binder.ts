import { Catalog } from "../catalog/catalog";
import { Column } from "../catalog/column";
import { Schema } from "../catalog/schema";
import {
  BinaryOperationExpressionAST,
  CreateIndexStatementAST,
  CreateTableStatementAST,
  DeleteStatementAST,
  DropTableStatementAST,
  ExpressionAST,
  FunctionCallExpressionAST,
  InsertStatementAST,
  LiteralExpressionAST,
  PathExpressionAST,
  SelectStatementAST,
  StatementAST,
  TableReferenceAST,
  UnaryOperationExpressionAST,
  UpdateStatementAST,
} from "../parser/ast";
import { Type } from "../type/type";
import {
  BoundCreateTableStatement,
  BoundDeleteStatement,
  BoundExpression,
  BoundInsertStatement,
  BoundPathExpression,
  BoundSelectStatement,
  BoundBaseTableReference,
  BoundStatement,
  BoundTableReference,
  BoundUpdateStatement,
  BoundBinaryOperationExpression,
  BoundUnaryOperationExpression,
  BoundLiteralExpression,
  BoundJoinTableReference,
  BoundSubqueryTableReference,
  BoundDropTableStatement,
  BoundSelectElement,
  BoundFunctionCallExpression,
  BoundCreateIndexStatement,
} from "./bound";

type Scope =
  | BoundTableReference
  | { type: "select_elements"; selectElements: BoundSelectElement[] };

export class Binder {
  private scope: Scope | null = null;
  constructor(private _catalog: Catalog) {}
  async bind(ast: StatementAST): Promise<BoundStatement> {
    switch (ast.type) {
      case "create_table_statement":
        return this.bindCreateTable(ast);
      case "drop_table_statement":
        return this.bindDropTable(ast);
      case "create_index_statement":
        return this.bindCreateIndex(ast);
      case "select_statement":
        return this.bindSelect(ast);
      case "insert_statement":
        return this.bindInsert(ast);
      case "update_statement":
        return this.bindUpdate(ast);
      case "delete_statement":
        return this.bindDelete(ast);
      default:
        throw new Error("Unexpected statement type");
    }
  }
  bindCreateTable(ast: CreateTableStatementAST): BoundCreateTableStatement {
    const columns = ast.tableElements.map((tableElement) => {
      let columnType: Type;
      switch (tableElement.dataType) {
        case "INTEGER":
          columnType = Type.INTEGER;
          break;
        case "VARCHAR":
          columnType = Type.VARCHAR;
          break;
        case "BOOLEAN":
          columnType = Type.BOOLEAN;
          break;
        default:
          throw new Error("Unexpected column type");
      }
      return new Column(tableElement.columnName, columnType);
    });
    return {
      type: "create_table_statement",
      tableName: ast.tableName,
      schema: new Schema(columns),
    };
  }
  async bindDropTable(
    ast: DropTableStatementAST
  ): Promise<BoundDropTableStatement> {
    const tableReference = await this.bindTableReference({
      type: "base_table_reference",
      tableName: ast.tableName,
    });
    if (tableReference.type !== "base_table_reference") {
      throw new Error("Unexpected table reference type");
    }
    return {
      type: "drop_table_statement",
      tableReference,
    };
  }
  bindCreateIndex(ast: CreateIndexStatementAST): BoundCreateIndexStatement {
    return {
      type: "create_index_statement",
      indexName: ast.indexName,
      tableName: ast.tableName,
      columnName: ast.columnName,
    };
  }
  async bindSelect(ast: SelectStatementAST): Promise<BoundSelectStatement> {
    const tableReference = await this.bindTableReference(ast.tableReference);
    this.scope = tableReference;
    const selectElements = ast.selectElements.map((selectElement) => {
      return {
        expression: this.bindExpression(selectElement.expression),
        ...(selectElement.alias != null ? { alias: selectElement.alias } : {}),
      };
    });
    const condition =
      ast.condition != null ? this.bindExpression(ast.condition) : null;
    const groupBy =
      ast.groupBy != null
        ? ast.groupBy.map((item) => this.bindPathExpression(item))
        : null;
    const having = ast.having != null ? this.bindExpression(ast.having) : null;
    if (!ast.isAsterisk) {
      this.scope = {
        type: "select_elements",
        selectElements,
      };
    }
    const orderBy =
      ast.orderBy != null
        ? {
            sortKeys: ast.orderBy.sortKeys.map((sortKey) => {
              return {
                expression: this.bindPathExpression(sortKey.expression),
                direction: sortKey.direction,
              };
            }),
          }
        : null;
    const limit =
      ast.limit != null
        ? {
            count: this.bindExpression(ast.limit.count),
          }
        : null;
    return {
      type: "select_statement",
      isAsterisk: ast.isAsterisk,
      selectElements,
      tableReference,
      ...(condition != null ? { condition } : {}),
      ...(groupBy != null ? { groupBy } : {}),
      ...(having != null ? { having } : {}),
      ...(orderBy != null ? { orderBy } : {}),
      ...(limit != null ? { limit } : {}),
    };
  }
  async bindTableReference(
    ast: TableReferenceAST
  ): Promise<BoundTableReference> {
    switch (ast.type) {
      case "base_table_reference":
        const tableOid = await this._catalog.getOidByTableName(ast.tableName);
        const schema = await this._catalog.getSchemaByOid(tableOid);
        return {
          type: "base_table_reference",
          tableName: ast.tableName,
          ...(ast.alias != null ? { alias: ast.alias } : {}),
          tableOid,
          schema,
        };
      case "join_table_reference":
        const left = await this.bindTableReference(ast.left);
        const right = await this.bindTableReference(ast.right);
        return {
          type: "join_table_reference",
          joinType: ast.joinType,
          condition: ast.condition,
          left,
          right,
        };
      case "subquery_table_reference":
        const query = await this.bindSelect(ast.query);
        return {
          type: "subquery_table_reference",
          name: ast.name,
          query,
        };
    }
  }
  async bindInsert(ast: InsertStatementAST): Promise<BoundInsertStatement> {
    const tableReference = await this.bindTableReference({
      type: "base_table_reference",
      tableName: ast.tableName,
    });
    if (tableReference.type !== "base_table_reference") {
      throw new Error("Unexpected table reference type");
    }
    if (ast.values.length !== tableReference.schema.columns.length) {
      console.log(ast.values, tableReference.schema.columns);
      throw new Error("Number of values does not match number of columns");
    }
    const values = ast.values.map((value) => this.bindExpression(value));
    return {
      type: "insert_statement",
      tableReference,
      values,
    };
  }
  async bindUpdate(ast: UpdateStatementAST): Promise<BoundUpdateStatement> {
    const tableReference = await this.bindTableReference({
      type: "base_table_reference",
      tableName: ast.tableName,
    });
    if (tableReference.type !== "base_table_reference") {
      throw new Error("Unexpected table reference type");
    }
    this.scope = tableReference;
    const assignments = ast.assignments.map((assignment) => {
      const target = this.bindPathExpression(assignment.target);
      const value = this.bindExpression(assignment.value);
      return {
        target,
        value,
      };
    });
    return {
      type: "update_statement",
      tableReference,
      assignments,
      ...(ast.condition != null
        ? { condition: this.bindExpression(ast.condition) }
        : {}),
    };
  }
  async bindDelete(ast: DeleteStatementAST): Promise<BoundDeleteStatement> {
    const tableReference = await this.bindTableReference({
      type: "base_table_reference",
      tableName: ast.tableName,
    });
    if (tableReference.type !== "base_table_reference") {
      throw new Error("Unexpected table reference type");
    }
    this.scope = tableReference;
    return {
      type: "delete_statement",
      tableReference,
      ...(ast.condition != null
        ? { condition: this.bindExpression(ast.condition) }
        : {}),
    };
  }
  bindExpression(ast: ExpressionAST): BoundExpression {
    switch (ast.type) {
      case "binary_operation": {
        return this.bindBinaryOperationExpression(ast);
      }
      case "unary_operation": {
        return this.bindUnaryOperationExpression(ast);
      }
      case "function_call": {
        return this.bindFunctionCallExpression(ast);
      }
      case "literal": {
        return this.bindLiteralExpression(ast);
      }
      case "path": {
        return this.bindPathExpression(ast);
      }
    }
  }
  bindBinaryOperationExpression(
    ast: BinaryOperationExpressionAST
  ): BoundBinaryOperationExpression {
    return {
      type: "binary_operation",
      operator: ast.operator,
      left: this.bindExpression(ast.left),
      right: this.bindExpression(ast.right),
    };
  }
  bindUnaryOperationExpression(
    ast: UnaryOperationExpressionAST
  ): BoundUnaryOperationExpression {
    return {
      type: "unary_operation",
      operator: ast.operator,
      operand: this.bindExpression(ast.operand),
    };
  }
  bindFunctionCallExpression(
    ast: FunctionCallExpressionAST
  ): BoundFunctionCallExpression {
    return {
      type: "function_call",
      functionName: ast.functionName,
      args: ast.args.map((arg) => this.bindExpression(arg)),
    };
  }
  bindLiteralExpression(ast: LiteralExpressionAST): BoundLiteralExpression {
    return {
      type: "literal",
      value: ast.value,
    };
  }
  bindPathExpression(ast: PathExpressionAST): BoundPathExpression {
    if (this.scope === null) {
      throw new Error("Scope is null");
    }
    if (this.scope.type === "select_elements") {
      const expression = this.resolvePathExpressionFromSelectElements(
        this.scope.selectElements,
        ast.path
      );
      if (expression === null) {
        throw new Error("Path not found");
      }
      return expression;
    }

    const expression = this.resolvePathExpressionFromTableReference(
      this.scope,
      ast.path
    );
    if (expression == null) {
      throw new Error("Path not found");
    }
    return expression;
  }
  resolvePathExpressionFromTableReference(
    table: BoundTableReference,
    path: string[]
  ): BoundPathExpression | null {
    switch (table.type) {
      case "base_table_reference":
        return this.resolvePathExpressionFromBaseTableReference(table, path);
      case "join_table_reference":
        return this.resolveColumnReferenceFromJoinTableReference(table, path);
      case "subquery_table_reference":
        return this.resolveColumnReferenceFromSubqueryTableReference(
          table,
          path
        );
    }
  }
  resolvePathExpressionFromBaseTableReference(
    table: BoundBaseTableReference,
    path: string[]
  ): BoundPathExpression | null {
    const directResolvedIndex = table.schema.columns.findIndex(
      (column) => column.name === path[0]
    );
    const tableName = table.alias != null ? table.alias : table.tableName;
    const stripResolvedIndex =
      path[0] === tableName
        ? table.schema.columns.findIndex((column) => column.name === path[1])
        : -1;
    if (directResolvedIndex !== -1 && stripResolvedIndex !== -1) {
      throw new Error("Ambiguous column reference");
    }
    if (stripResolvedIndex !== -1) {
      return {
        type: "path",
        path,
      };
    }
    if (directResolvedIndex !== -1) {
      return {
        type: "path",
        path: [...(tableName === "" ? [] : [tableName]), path[0]],
      };
    }
    return null;
  }
  resolveColumnReferenceFromJoinTableReference(
    table: BoundJoinTableReference,
    path: string[]
  ): BoundPathExpression | null {
    const leftResolved = this.resolvePathExpressionFromTableReference(
      table.left,
      path
    );
    const rightResolved = this.resolvePathExpressionFromTableReference(
      table.right,
      path
    );
    if (leftResolved != null && rightResolved != null) {
      throw new Error("Ambiguous column reference");
    }
    if (leftResolved != null) {
      return leftResolved;
    }
    if (rightResolved != null) {
      return rightResolved;
    }
    return null;
  }
  resolveColumnReferenceFromSubqueryTableReference(
    table: BoundSubqueryTableReference,
    path: string[]
  ): BoundPathExpression | null {
    if (path.length !== 2) {
      return null;
    }
    if (path[0] !== table.name) {
      return null;
    }
    return this.resolvePathExpressionFromSelectElements(
      table.query.selectElements,
      path.slice(1)
    );
  }
  resolvePathExpressionFromSelectElements(
    selectElements: BoundSelectElement[],
    path: string[]
  ): BoundPathExpression | null {
    const resolved = selectElements.find((selectElement) => {
      const columnName =
        selectElement.alias != null
          ? selectElement.alias
          : selectElement.expression.type === "path"
          ? selectElement.expression.path[
              selectElement.expression.path.length - 1
            ]
          : "";
      return columnName === path[0];
    });
    if (resolved == null || resolved.expression.type !== "path") {
      return null;
    }
    return {
      type: "path",
      path: [
        resolved.alias != null
          ? resolved.alias
          : resolved.expression.path[resolved.expression.path.length - 1],
      ],
    };
  }
}
