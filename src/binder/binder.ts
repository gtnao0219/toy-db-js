import { Catalog } from "../catalog/catalog";
import { Column } from "../catalog/column";
import { Schema } from "../catalog/schema";
import {
  CreateTableStatementAST,
  DeleteStatementAST,
  ExpressionAST,
  InsertStatementAST,
  PathExpressionAST,
  SelectStatementAST,
  StatementAST,
  TableReferenceAST,
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
  BoundSimpleTableReference,
  BoundStatement,
  BoundTableReference,
  BoundUpdateStatement,
} from "./bound";

export class Binder {
  private scope: BoundTableReference | null = null;
  constructor(private _catalog: Catalog) {}
  async bind(ast: StatementAST): Promise<BoundStatement> {
    switch (ast.type) {
      case "create_table_statement":
        return this.bindCreateTable(ast);
      case "insert_statement":
        return this.bindInsert(ast);
      case "delete_statement":
        return this.bindDelete(ast);
      case "update_statement":
        return this.bindUpdate(ast);
      case "select_statement":
        return this.bindSelect(ast);
      default:
        throw new Error("Unexpected statement type");
    }
  }
  bindCreateTable(ast: CreateTableStatementAST): BoundCreateTableStatement {
    const columns = ast.tableElements.map((tableElement) => {
      let columnType: Type;
      switch (tableElement.columnType) {
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
  async bindInsert(ast: InsertStatementAST): Promise<BoundInsertStatement> {
    const tableOid = await this._catalog.getOidByTableName(ast.tableName);
    const schema = await this._catalog.getSchemaByOid(tableOid);
    if (ast.values.length !== schema.columns.length) {
      throw new Error("Number of values does not match number of columns");
    }
    return {
      type: "insert_statement",
      tableOid,
      schema,
      values: ast.values,
    };
  }
  async bindDelete(ast: DeleteStatementAST): Promise<BoundDeleteStatement> {
    const tableOid = await this._catalog.getOidByTableName(ast.tableName);
    const schema = await this._catalog.getSchemaByOid(tableOid);
    return {
      type: "delete_statement",
      tableOid,
      schema,
      condition: ast.condition,
    };
  }
  async bindUpdate(ast: UpdateStatementAST): Promise<BoundUpdateStatement> {
    const tableOid = await this._catalog.getOidByTableName(ast.tableName);
    const schema = await this._catalog.getSchemaByOid(tableOid);
    const assignments = ast.assignments.map((assignment) => {
      const columnIndex = schema.columns.findIndex(
        (column) => column.name === assignment.columnName
      );
      if (columnIndex === -1) {
        throw new Error("Column not found");
      }
      return {
        columnIndex,
        value: assignment.value,
      };
    });
    return {
      type: "update_statement",
      tableOid,
      schema,
      assignments,
      condition: ast.condition,
    };
  }
  async bindSelect(ast: SelectStatementAST): Promise<BoundSelectStatement> {
    const tableReference = await this.bindTableReference(ast.tableReference);
    this.scope = tableReference;
    return {
      type: "select_statement",
      isAsterisk: ast.isAsterisk,
      selectElements: ast.selectElements.map((selectElement) => {
        return {
          expression: this.bindExpression(selectElement.expression),
          alias: selectElement.alias,
        };
      }),
      tableReference,
      condition: ast.condition,
      orderBy: ast.orderBy,
      limit: ast.limit,
    };
  }
  async bindTableReference(
    ast: TableReferenceAST
  ): Promise<BoundTableReference> {
    switch (ast.type) {
      case "simple_table_reference":
        const tableOid = await this._catalog.getOidByTableName(ast.tableName);
        const schema = await this._catalog.getSchemaByOid(tableOid);
        return {
          type: "simple_table_reference",
          tableName: ast.tableName,
          alias: ast.alias,
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
  bindExpression(ast: ExpressionAST): BoundExpression {
    switch (ast.type) {
      case "path": {
        return this.resolvePathExpression(ast);
      }
      default: {
        throw new Error("Unexpected expression type");
      }
    }
  }
  resolvePathExpression(ast: PathExpressionAST): BoundPathExpression {
    if (this.scope === null) {
      throw new Error("Scope is null");
    }
    switch (this.scope.type) {
      case "simple_table_reference":
        return this.resolveColumnReferenceFromBaseTableReference(
          ast,
          this.scope
        );
      // case "join_table_reference":
      //   return this.resolveColumnReferenceFromJoinTableReference(ast);
      // case "subquery_table_reference":
      //   return this.resolveColumnReferenceFromSubqueryTableReference(ast);
      default:
        throw new Error("Unexpected table reference type");
    }
  }
  resolveColumnReferenceFromBaseTableReference(
    ast: PathExpressionAST,
    scope: BoundSimpleTableReference
  ): BoundPathExpression {
    const directResolvedIndex = scope.schema.columns.findIndex(
      (column) => column.name === ast.path[0]
    );
    // TODO: alias
    const stripResolvedIndex =
      ast.path[0] === scope.tableName
        ? scope.schema.columns.findIndex(
            (column) => column.name === ast.path[1]
          )
        : undefined;
    if (directResolvedIndex !== -1 && stripResolvedIndex !== -1) {
      throw new Error("Ambiguous column reference");
    }
    if (stripResolvedIndex !== -1) {
      return {
        type: "path_expression",
        path: ast.path,
      };
    }
    return {
      type: "path_expression",
      path: [scope.tableName, ast.path[0]],
    };
  }
}
