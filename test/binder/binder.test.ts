import { Binder } from "../../src/binder/binder";
import { ICatalog } from "../../src/catalog/catalog";
import { Column } from "../../src/catalog/column";
import { Schema } from "../../src/catalog/schema";
import {
  DeleteStatementAST,
  InsertStatementAST,
  SelectStatementAST,
  UpdateStatementAST,
} from "../../src/parser/ast";
import { Type } from "../../src/type/type";

class MockCatalog implements ICatalog {
  async getOidByTableName(tableName: string): Promise<number> {
    switch (tableName) {
      case "foo":
        return 1;
      case "bar":
        return 2;
      default:
        throw new Error("Table not found");
    }
  }
  async getSchemaByOid(oid: number): Promise<Schema> {
    switch (oid) {
      case 1:
        return new Schema([
          new Column("id", Type.INTEGER),
          new Column("name", Type.VARCHAR),
        ]);
      case 2:
        return new Schema([
          new Column("id", Type.INTEGER),
          new Column("name", Type.VARCHAR),
          new Column("foo_id", Type.INTEGER),
        ]);
      default:
        throw new Error("Table not found");
    }
  }
}
describe("Binder", () => {
  const binder = new Binder(new MockCatalog());
  describe("bind", () => {
    it("should bind a select statement", async () => {
      const selectStatement: SelectStatementAST = {
        type: "select_statement",
        isAsterisk: true,
        selectElements: [],
        tableReference: {
          type: "base_table_reference",
          tableName: "foo",
          alias: "bar",
        },
      };
      const boundStatement = await binder.bind(selectStatement);
      expect(boundStatement).toEqual({
        type: "select_statement",
        isAsterisk: true,
        selectElements: [],
        tableReference: {
          type: "base_table_reference",
          tableName: "foo",
          alias: "bar",
          tableOid: 1,
          schema: new Schema([
            new Column("id", Type.INTEGER),
            new Column("name", Type.VARCHAR),
          ]),
        },
      });
    });
    it("should bind a select statement with a column", async () => {
      const selectStatement: SelectStatementAST = {
        type: "select_statement",
        isAsterisk: false,
        selectElements: [
          {
            expression: {
              type: "path",
              path: ["id"],
            },
            alias: "_id",
          },
          {
            expression: {
              type: "path",
              path: ["bar", "name"],
            },
          },
        ],
        tableReference: {
          type: "base_table_reference",
          tableName: "foo",
          alias: "bar",
        },
      };
      const boundStatement = await binder.bind(selectStatement);
      expect(boundStatement).toEqual({
        type: "select_statement",
        isAsterisk: false,
        selectElements: [
          {
            expression: {
              type: "path",
              path: ["bar", "id"],
            },
            alias: "_id",
          },
          {
            expression: {
              type: "path",
              path: ["bar", "name"],
            },
          },
        ],
        tableReference: {
          type: "base_table_reference",
          tableName: "foo",
          alias: "bar",
          tableOid: 1,
          schema: new Schema([
            new Column("id", Type.INTEGER),
            new Column("name", Type.VARCHAR),
          ]),
        },
      });
    });
    it("should bind a select statement with a where clause", async () => {
      const selectStatement: SelectStatementAST = {
        type: "select_statement",
        isAsterisk: true,
        selectElements: [],
        tableReference: {
          type: "base_table_reference",
          tableName: "foo",
          alias: "bar",
        },
        condition: {
          type: "binary_operation",
          operator: "=",
          left: {
            type: "path",
            path: ["id"],
          },
          right: {
            type: "literal",
            value: 1,
          },
        },
      };
      const boundStatement = await binder.bind(selectStatement);
      expect(boundStatement).toEqual({
        type: "select_statement",
        isAsterisk: true,
        selectElements: [],
        tableReference: {
          type: "base_table_reference",
          tableName: "foo",
          alias: "bar",
          tableOid: 1,
          schema: new Schema([
            new Column("id", Type.INTEGER),
            new Column("name", Type.VARCHAR),
          ]),
        },
        condition: {
          type: "binary_operation",
          operator: "=",
          left: {
            type: "path",
            path: ["bar", "id"],
          },
          right: {
            type: "literal",
            value: 1,
          },
        },
      });
    });
    it("should bind a select statement with a join", async () => {
      const selectStatement: SelectStatementAST = {
        type: "select_statement",
        isAsterisk: false,
        selectElements: [
          {
            expression: {
              type: "path",
              path: ["foo", "id"],
            },
          },
          {
            expression: {
              type: "path",
              path: ["foo_id"],
            },
          },
        ],
        tableReference: {
          type: "join_table_reference",
          joinType: "INNER",
          left: {
            type: "base_table_reference",
            tableName: "foo",
          },
          right: {
            type: "base_table_reference",
            tableName: "bar",
            alias: "baz",
          },
          condition: {
            type: "binary_operation",
            operator: "=",
            left: {
              type: "path",
              path: ["foo", "id"],
            },
            right: {
              type: "path",
              path: ["baz", "foo_id"],
            },
          },
        },
      };
      const boundStatement = await binder.bind(selectStatement);
      expect(boundStatement).toEqual({
        type: "select_statement",
        isAsterisk: false,
        selectElements: [
          {
            expression: {
              type: "path",
              path: ["foo", "id"],
            },
          },
          {
            expression: {
              type: "path",
              path: ["baz", "foo_id"],
            },
          },
        ],
        tableReference: {
          type: "join_table_reference",
          joinType: "INNER",
          left: {
            type: "base_table_reference",
            tableName: "foo",
            tableOid: 1,
            schema: new Schema([
              new Column("id", Type.INTEGER),
              new Column("name", Type.VARCHAR),
            ]),
          },
          right: {
            type: "base_table_reference",
            tableName: "bar",
            alias: "baz",
            tableOid: 2,
            schema: new Schema([
              new Column("id", Type.INTEGER),
              new Column("name", Type.VARCHAR),
              new Column("foo_id", Type.INTEGER),
            ]),
          },
          condition: {
            type: "binary_operation",
            operator: "=",
            left: {
              type: "path",
              path: ["foo", "id"],
            },
            right: {
              type: "path",
              path: ["baz", "foo_id"],
            },
          },
        },
      });
    });
    it("should bind a select statement with order by and limit clause", async () => {
      const selectStatement: SelectStatementAST = {
        type: "select_statement",
        isAsterisk: true,
        selectElements: [],
        tableReference: {
          type: "base_table_reference",
          tableName: "foo",
        },
        orderBy: {
          sortKeys: [
            {
              expression: {
                type: "path",
                path: ["id"],
              },
              direction: "ASC",
            },
            {
              expression: {
                type: "path",
                path: ["name"],
              },
              direction: "DESC",
            },
          ],
        },
        limit: {
          count: 1,
        },
      };
      const boundStatement = await binder.bind(selectStatement);
      expect(boundStatement).toEqual({
        type: "select_statement",
        isAsterisk: true,
        selectElements: [],
        tableReference: {
          type: "base_table_reference",
          tableName: "foo",
          tableOid: 1,
          schema: new Schema([
            new Column("id", Type.INTEGER),
            new Column("name", Type.VARCHAR),
          ]),
        },
        orderBy: {
          sortKeys: [
            {
              expression: {
                type: "path",
                path: ["foo", "id"],
              },
              direction: "ASC",
            },
            {
              expression: {
                type: "path",
                path: ["foo", "name"],
              },
              direction: "DESC",
            },
          ],
        },
        limit: {
          count: 1,
        },
      });
    });
    it("should bind a insert statement", async () => {
      const insertStatement: InsertStatementAST = {
        type: "insert_statement",
        tableName: "foo",
        values: [
          {
            type: "literal",
            value: 1,
          },
          {
            type: "literal",
            value: "foo",
          },
        ],
      };
      const boundStatement = await binder.bind(insertStatement);
      expect(boundStatement).toEqual({
        type: "insert_statement",
        tableReference: {
          type: "base_table_reference",
          tableName: "foo",
          tableOid: 1,
          schema: new Schema([
            new Column("id", Type.INTEGER),
            new Column("name", Type.VARCHAR),
          ]),
        },
        values: [
          {
            type: "literal",
            value: 1,
          },
          {
            type: "literal",
            value: "foo",
          },
        ],
      });
    });
    it("should bind a update statement", async () => {
      const updateStatement: UpdateStatementAST = {
        type: "update_statement",
        tableName: "foo",
        assignments: [
          {
            columnName: "name",
            value: {
              type: "literal",
              value: "qux",
            },
          },
        ],
        condition: {
          type: "binary_operation",
          operator: "=",
          left: {
            type: "path",
            path: ["id"],
          },
          right: {
            type: "literal",
            value: 1,
          },
        },
      };
      const boundStatement = await binder.bind(updateStatement);
      expect(boundStatement).toEqual({
        type: "update_statement",
        tableReference: {
          type: "base_table_reference",
          tableName: "foo",
          tableOid: 1,
          schema: new Schema([
            new Column("id", Type.INTEGER),
            new Column("name", Type.VARCHAR),
          ]),
        },
        assignments: [
          {
            columnIndex: 1,
            value: {
              type: "literal",
              value: "qux",
            },
          },
        ],
        condition: {
          type: "binary_operation",
          operator: "=",
          left: {
            type: "path",
            path: ["foo", "id"],
          },
          right: {
            type: "literal",
            value: 1,
          },
        },
      });
    });
    it("should bind a delete statement", async () => {
      const deleteStatement: DeleteStatementAST = {
        type: "delete_statement",
        tableName: "foo",
        condition: {
          type: "binary_operation",
          operator: "=",
          left: {
            type: "path",
            path: ["id"],
          },
          right: {
            type: "literal",
            value: 1,
          },
        },
      };
      const boundStatement = await binder.bind(deleteStatement);
      expect(boundStatement).toEqual({
        type: "delete_statement",
        tableReference: {
          type: "base_table_reference",
          tableName: "foo",
          tableOid: 1,
          schema: new Schema([
            new Column("id", Type.INTEGER),
            new Column("name", Type.VARCHAR),
          ]),
        },
        condition: {
          type: "binary_operation",
          operator: "=",
          left: {
            type: "path",
            path: ["foo", "id"],
          },
          right: {
            type: "literal",
            value: 1,
          },
        },
      });
    });
  });
});
