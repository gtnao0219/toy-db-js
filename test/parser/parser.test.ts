import { Parser } from "../../src/parser/parser";

describe("Parser", () => {
  describe("parse", () => {
    describe("select_statement", () => {
      it("should parse a simple select statement", () => {
        const sql = "SELECT * FROM users";
        const parser = new Parser(sql);
        const ast = parser.parse();
        expect(ast).toEqual({
          type: "select_statement",
          isAsterisk: true,
          selectElements: [],
          tableName: "users",
        });
      });
      it("should parse a select statement with select elements", () => {
        const sql = "SELECT id, name FROM users";
        const parser = new Parser(sql);
        const ast = parser.parse();
        expect(ast).toEqual({
          type: "select_statement",
          isAsterisk: false,
          selectElements: [
            { expression: { type: "path", path: ["id"] } },
            { expression: { type: "path", path: ["name"] } },
          ],
          tableName: "users",
        });
      });
      it("should parse a select statement with various select elements", () => {
        const sql = "SELECT id AS _id, users.name, 'foo' + 'bar' FROM users";
        const parser = new Parser(sql);
        const ast = parser.parse();
        expect(ast).toEqual({
          type: "select_statement",
          isAsterisk: false,
          selectElements: [
            { expression: { type: "path", path: ["id"] }, alias: "_id" },
            { expression: { type: "path", path: ["users", "name"] } },
            {
              expression: {
                type: "binary_operation",
                operator: "+",
                left: { type: "literal", value: "foo" },
                right: { type: "literal", value: "bar" },
              },
            },
          ],
          tableName: "users",
        });
      });
      it("should parse a select statement with where clause", () => {
        const sql = "SELECT * FROM users WHERE id = 1";
        const parser = new Parser(sql);
        const ast = parser.parse();
        expect(ast).toEqual({
          type: "select_statement",
          isAsterisk: true,
          selectElements: [],
          tableName: "users",
          condition: {
            type: "binary_operation",
            operator: "=",
            left: { type: "path", path: ["id"] },
            right: { type: "literal", value: 1 },
          },
        });
      });
      it("should parse a select statement with order by clause", () => {
        const sql = "SELECT * FROM users ORDER BY id, name DESC";
        const parser = new Parser(sql);
        const ast = parser.parse();
        expect(ast).toEqual({
          type: "select_statement",
          isAsterisk: true,
          selectElements: [],
          tableName: "users",
          orderBy: {
            sortKeys: [
              {
                columnName: "id",
                direction: "ASC",
              },
              {
                columnName: "name",
                direction: "DESC",
              },
            ],
          },
        });
      });
      it("should parse a select statement with limit clause", () => {
        const sql = "SELECT * FROM users LIMIT 10";
        const parser = new Parser(sql);
        const ast = parser.parse();
        expect(ast).toEqual({
          type: "select_statement",
          isAsterisk: true,
          selectElements: [],
          tableName: "users",
          limit: {
            value: 10,
          },
        });
      });
    });
    describe("insert_statement", () => {
      it("should parse a simple insert statement", () => {
        const sql = "INSERT INTO users VALUES (1, 'foo')";
        const parser = new Parser(sql);
        const ast = parser.parse();
        expect(ast).toEqual({
          type: "insert_statement",
          tableName: "users",
          values: [
            { type: "literal", value: 1 },
            { type: "literal", value: "foo" },
          ],
        });
      });
    });
    describe("update_statement", () => {
      it("should parse a simple update statement", () => {
        const sql = "UPDATE users SET name = 'foo' WHERE id = 1";
        const parser = new Parser(sql);
        const ast = parser.parse();
        expect(ast).toEqual({
          type: "update_statement",
          tableName: "users",
          assignments: [
            {
              columnName: "name",
              value: { type: "literal", value: "foo" },
            },
          ],
          condition: {
            type: "binary_operation",
            operator: "=",
            left: { type: "path", path: ["id"] },
            right: { type: "literal", value: 1 },
          },
        });
      });
    });
    describe("delete_statement", () => {
      it("should parse a simple delete statement", () => {
        const sql = "DELETE FROM users WHERE id = 1";
        const parser = new Parser(sql);
        const ast = parser.parse();
        expect(ast).toEqual({
          type: "delete_statement",
          tableName: "users",
          condition: {
            type: "binary_operation",
            operator: "=",
            left: { type: "path", path: ["id"] },
            right: { type: "literal", value: 1 },
          },
        });
      });
    });
    describe("create_table_statement", () => {
      it("should parse a simple create table statement", () => {
        const sql = "CREATE TABLE users (id INTEGER, name VARCHAR)";
        const parser = new Parser(sql);
        const ast = parser.parse();
        expect(ast).toEqual({
          type: "create_table_statement",
          tableName: "users",
          tableElements: [
            { columnName: "id", columnType: "INTEGER" },
            { columnName: "name", columnType: "VARCHAR" },
          ],
        });
      });
    });
    describe("drop_table_statement", () => {
      it("should parse a simple drop table statement", () => {
        const sql = "DROP TABLE users";
        const parser = new Parser(sql);
        const ast = parser.parse();
        expect(ast).toEqual({
          type: "drop_table_statement",
          tableName: "users",
        });
      });
    });
    describe("begin_statement", () => {
      it("should parse a simple begin statement", () => {
        const sql = "BEGIN";
        const parser = new Parser(sql);
        const ast = parser.parse();
        expect(ast).toEqual({
          type: "begin_statement",
        });
      });
    });
    describe("commit_statement", () => {
      it("should parse a simple commit statement", () => {
        const sql = "COMMIT";
        const parser = new Parser(sql);
        const ast = parser.parse();
        expect(ast).toEqual({
          type: "commit_statement",
        });
      });
    });
    describe("rollback_statement", () => {
      it("should parse a simple rollback statement", () => {
        const sql = "ROLLBACK";
        const parser = new Parser(sql);
        const ast = parser.parse();
        expect(ast).toEqual({
          type: "rollback_statement",
        });
      });
    });
  });
});
