import { ICatalog } from "../catalog/catalog";
import { Column } from "../catalog/column";
import { Schema } from "../catalog/schema";
import {
  BinaryOperationExpressionAST,
  CreateTableStatementAST,
  DeleteStatementAST,
  ExpressionAST,
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
} from "./bound";

export class Binder {
  private scope: BoundTableReference | null = null;
  constructor(private _catalog: ICatalog) {}
  async bind(ast: StatementAST): Promise<BoundStatement> {
    switch (ast.type) {
      case "create_table_statement":
        return this.bindCreateTable(ast);
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
      ast.condition != null ? this.bindExpression(ast.condition) : undefined;
    // TODO: scope change?
    const orderBy =
      ast.orderBy != null
        ? {
            sortKeys: ast.orderBy.sortKeys.map((sortKey) => {
              return {
                expression: this.bindExpression(sortKey.expression),
                direction: sortKey.direction,
              };
            }),
          }
        : undefined;
    return {
      type: "select_statement",
      isAsterisk: ast.isAsterisk,
      selectElements,
      tableReference,
      ...(condition != null ? { condition } : {}),
      ...(orderBy != null ? { orderBy } : {}),
      ...(ast.limit != null ? { limit: ast.limit } : {}),
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
    const tableOid = await this._catalog.getOidByTableName(ast.tableName);
    const schema = await this._catalog.getSchemaByOid(tableOid);
    if (ast.values.length !== schema.columns.length) {
      throw new Error("Number of values does not match number of columns");
    }
    const values = ast.values.map((value) => this.bindExpression(value));
    return {
      type: "insert_statement",
      tableReference: {
        type: "base_table_reference",
        tableName: ast.tableName,
        tableOid,
        schema,
      },
      values,
    };
  }
  async bindUpdate(ast: UpdateStatementAST): Promise<BoundUpdateStatement> {
    const tableOid = await this._catalog.getOidByTableName(ast.tableName);
    const schema = await this._catalog.getSchemaByOid(tableOid);
    this.scope = {
      type: "base_table_reference",
      tableName: ast.tableName,
      tableOid,
      schema,
    };
    const assignments = ast.assignments.map((assignment) => {
      const columnIndex = schema.columns.findIndex(
        (column) => column.name === assignment.columnName
      );
      if (columnIndex === -1) {
        throw new Error("Column not found");
      }
      const value = this.bindExpression(assignment.value);
      return {
        columnIndex,
        value,
      };
    });
    const condition =
      ast.condition != null ? this.bindExpression(ast.condition) : undefined;
    return {
      type: "update_statement",
      tableReference: {
        type: "base_table_reference",
        tableName: ast.tableName,
        tableOid,
        schema,
      },
      assignments,
      ...(condition != null ? { condition } : {}),
    };
  }
  async bindDelete(ast: DeleteStatementAST): Promise<BoundDeleteStatement> {
    const tableOid = await this._catalog.getOidByTableName(ast.tableName);
    const schema = await this._catalog.getSchemaByOid(tableOid);
    this.scope = {
      type: "base_table_reference",
      tableName: ast.tableName,
      tableOid,
      schema,
    };
    const condition =
      ast.condition != null ? this.bindExpression(ast.condition) : undefined;
    return {
      type: "delete_statement",
      tableReference: {
        type: "base_table_reference",
        tableName: ast.tableName,
        tableOid,
        schema,
      },
      ...(condition != null ? { condition } : {}),
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
      case "literal": {
        return this.bindLiteralExpression(ast);
      }
      case "path": {
        return this.bindPathExpression(ast);
      }
      default: {
        throw new Error("Unexpected expression type");
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
  bindLiteralExpression(ast: LiteralExpressionAST): BoundLiteralExpression {
    // TODO: value
    return {
      type: "literal",
      value: ast.value,
    };
  }
  bindPathExpression(ast: PathExpressionAST): BoundPathExpression {
    if (this.scope === null) {
      throw new Error("Scope is null");
    }
    const expression = this.resolvePathExpressionFromTableReference(
      this.scope,
      ast.path
    );
    if (expression == null) {
      throw new Error("Expression is null");
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
      default:
        throw new Error("Unexpected table reference type");
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
        path: [tableName, path[0]],
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
    throw new Error("Not implemented");
  }
}
