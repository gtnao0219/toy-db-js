import { Catalog } from "../catalog/catalog";
import { Column } from "../catalog/column";
import { Schema } from "../catalog/schema";
import {
  AST,
  CreateTableStmtAST,
  DeleteStmtAST,
  InsertStmtAST,
  SelectStmtAST,
  UpdateStmtAST,
} from "../parser/ast";
import { BooleanValue } from "../type/boolean_value";
import { IntegerValue } from "../type/integer_value";
import { Type } from "../type/type";
import { Value } from "../type/value";
import { VarcharValue } from "../type/varchar_value";
import {
  BoundBinaryExpression,
  BoundColumnRefExpression,
  BoundExpression,
  BoundLiteralExpression,
} from "./bound_expression";
import { BoundBaseTableRef } from "./bound_table_ref";
import { CreateTableStatement } from "./statement/create_table_statement";
import { DeleteStatement } from "./statement/delete_statement";
import { InsertStatement } from "./statement/insert_statement";
import { SelectStatement } from "./statement/select_statement";
import { Statement } from "./statement/statement";
import { UpdateStatement } from "./statement/update_statement";

export class Binder {
  constructor(private _catalog: Catalog) {}
  async bind(ast: AST): Promise<Statement> {
    switch (ast.type) {
      case "create_table_stmt":
        return this.bindCreateTable(ast as CreateTableStmtAST);
      case "insert_stmt":
        return await this.bindInsert(ast as InsertStmtAST);
      case "delete_stmt":
        return await this.bindDelete(ast as DeleteStmtAST);
      case "update_stmt":
        return await this.bindUpdate(ast as UpdateStmtAST);
      case "select_stmt":
        return await this.bindSelect(ast as SelectStmtAST);
      default:
        throw new Error("Unexpected statement type");
    }
  }
  bindCreateTable(ast: CreateTableStmtAST): CreateTableStatement {
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
    return new CreateTableStatement(ast.tableName, new Schema(columns));
  }
  async bindInsert(ast: InsertStmtAST): Promise<InsertStatement> {
    const tableOid = await this._catalog.getOidByTableName(ast.tableName);
    const schema = await this._catalog.getSchemaByOid(tableOid);
    const tableRef = new BoundBaseTableRef(tableOid, schema);
    if (ast.values.length !== schema.columns.length) {
      throw new Error("Number of values does not match number of columns");
    }
    const values = ast.values.map((value, index) => {
      const column = schema.columns[index];
      if (column.type === Type.INTEGER && typeof value === "number") {
        return new IntegerValue(value);
      }
      if (column.type === Type.VARCHAR && typeof value === "string") {
        return new VarcharValue(value);
      }
      if (column.type === Type.BOOLEAN && typeof value === "boolean") {
        return new BooleanValue(value);
      }
      throw new Error(`Type mismatch: ${column.type} and ${typeof value}`);
    });
    return new InsertStatement(tableRef, values);
  }
  async bindDelete(ast: DeleteStmtAST): Promise<DeleteStatement> {
    const tableOid = await this._catalog.getOidByTableName(ast.tableName);
    const schema = await this._catalog.getSchemaByOid(tableOid);
    const tableRef = new BoundBaseTableRef(tableOid, schema);
    let predicate: BoundExpression = new BoundLiteralExpression(true);
    const condition = ast.condition;
    if (condition != null) {
      const columnIndex = schema.columns.findIndex(
        (column) => column.name === condition.columnName
      );
      if (columnIndex === -1) {
        throw new Error(`Column ${condition.columnName} does not exist`);
      }
      predicate = new BoundBinaryExpression(
        new BoundColumnRefExpression(tableRef, columnIndex),
        "=",
        new BoundLiteralExpression(condition.right)
      );
    }
    return new DeleteStatement(tableRef, predicate);
  }
  async bindUpdate(ast: UpdateStmtAST): Promise<UpdateStatement> {
    const tableOid = await this._catalog.getOidByTableName(ast.tableName);
    const schema = await this._catalog.getSchemaByOid(tableOid);
    const tableRef = new BoundBaseTableRef(tableOid, schema);
    const assignments = ast.assignments.map((assignment) => {
      const columnIndex = schema.columns.findIndex(
        (column) => column.name === assignment.columnName
      );
      if (columnIndex === -1) {
        throw new Error(`Column ${assignment.columnName} does not exist`);
      }
      const column = schema.columns[columnIndex];
      let value: Value;
      if (
        column.type === Type.INTEGER &&
        typeof assignment.value === "number"
      ) {
        value = new IntegerValue(assignment.value);
      } else if (
        column.type === Type.VARCHAR &&
        typeof assignment.value === "string"
      ) {
        value = new VarcharValue(assignment.value);
      } else if (
        column.type === Type.BOOLEAN &&
        typeof assignment.value === "boolean"
      ) {
        value = new BooleanValue(assignment.value);
      } else {
        throw new Error(
          `Type mismatch: ${column.type} and ${typeof assignment.value}`
        );
      }
      return {
        columnIndex,
        value,
      };
    });
    let predicate: BoundExpression = new BoundLiteralExpression(true);
    const condition = ast.condition;
    if (condition != null) {
      const columnIndex = schema.columns.findIndex(
        (column) => column.name === condition.columnName
      );
      if (columnIndex === -1) {
        throw new Error(`Column ${condition.columnName} does not exist`);
      }
      predicate = new BoundBinaryExpression(
        new BoundColumnRefExpression(tableRef, columnIndex),
        "=",
        new BoundLiteralExpression(condition.right)
      );
    }
    return new UpdateStatement(tableRef, assignments, predicate);
  }
  async bindSelect(ast: SelectStmtAST): Promise<SelectStatement> {
    const tableOid = await this._catalog.getOidByTableName(ast.tableName);
    const schema = await this._catalog.getSchemaByOid(tableOid);
    const tableRef = new BoundBaseTableRef(tableOid, schema);
    let predicate: BoundExpression = new BoundLiteralExpression(true);
    const condition = ast.condition;
    if (condition != null) {
      const columnIndex = schema.columns.findIndex(
        (column) => column.name === condition.columnName
      );
      if (columnIndex === -1) {
        throw new Error(`Column ${condition.columnName} does not exist`);
      }
      predicate = new BoundBinaryExpression(
        new BoundColumnRefExpression(tableRef, columnIndex),
        "=",
        new BoundLiteralExpression(condition.right)
      );
    }
    const sortKeys = [];
    const orderBy = ast.orderBy;
    if (orderBy != null) {
      for (const sk of orderBy.sortKeys) {
        const columnIndex = schema.columns.findIndex(
          (c) => c.name === sk.columnName
        );
        if (columnIndex === -1) {
          throw new Error(`Column ${sk.columnName} does not exist`);
        }
        sortKeys.push({
          columnIndex,
          direction: sk.direction,
        });
      }
    }
    const limit = ast.limit == null ? null : ast.limit.value;
    if (ast.isAsterisk) {
      return new SelectStatement(
        tableRef,
        schema.columns.map((_, i) => {
          return new BoundColumnRefExpression(tableRef, i);
        }),
        predicate,
        sortKeys,
        limit
      );
    }
    const expressions = ast.columnNames.map((columnName) => {
      const columnIndex = schema.columns.findIndex(
        (column) => column.name === columnName
      );
      if (columnIndex === -1) {
        throw new Error(`Column ${columnName} does not exist`);
      }
      return new BoundColumnRefExpression(tableRef, columnIndex);
    });
    return new SelectStatement(
      tableRef,
      expressions,
      predicate,
      sortKeys,
      limit
    );
  }
}
