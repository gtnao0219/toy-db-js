import { SelectStatementAST } from "../../src/parser/ast";
import { Parser } from "../../src/parser/parser";

describe("Parser", () => {
  describe("select_statement", () => {
    it("should parse a simple select statement", () => {
      const sql = "SELECT * FROM users";
      const parser = new Parser(sql);
      const ast = parser.parse();
      expect(ast).toEqual({
        type: "select_statement",
        isAsterisk: true,
        selectElements: [],
        tableReference: {
          type: "base_table_reference",
          tableName: "users",
        },
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
        tableReference: {
          type: "base_table_reference",
          tableName: "users",
        },
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
        tableReference: {
          type: "base_table_reference",
          tableName: "users",
        },
      });
    });
    it("should parse a select statement with joined table", () => {
      const sql =
        "SELECT * FROM users INNER JOIN posts ON users.id = posts.user_id LEFT JOIN comments ON posts.id = comments.post_id";
      const parser = new Parser(sql);
      const ast = parser.parse();
      expect(ast).toEqual({
        type: "select_statement",
        isAsterisk: true,
        selectElements: [],
        tableReference: {
          type: "join_table_reference",
          joinType: "LEFT",
          left: {
            type: "join_table_reference",
            joinType: "INNER",
            left: {
              type: "base_table_reference",
              tableName: "users",
            },
            right: {
              type: "base_table_reference",
              tableName: "posts",
            },
            condition: {
              type: "binary_operation",
              operator: "=",
              left: {
                type: "path",
                path: ["users", "id"],
              },
              right: {
                type: "path",
                path: ["posts", "user_id"],
              },
            },
          },
          right: {
            type: "base_table_reference",
            tableName: "comments",
          },
          condition: {
            type: "binary_operation",
            operator: "=",
            left: {
              type: "path",
              path: ["posts", "id"],
            },
            right: {
              type: "path",
              path: ["comments", "post_id"],
            },
          },
        },
      });
    });
    it("should parse a select statement with subquery", () => {
      const sql = "SELECT * FROM (SELECT * FROM posts) AS t1";
      const parser = new Parser(sql);
      const ast = parser.parse();
      expect(ast).toEqual({
        type: "select_statement",
        isAsterisk: true,
        selectElements: [],
        tableReference: {
          type: "subquery_table_reference",
          query: {
            type: "select_statement",
            isAsterisk: true,
            selectElements: [],
            tableReference: {
              type: "base_table_reference",
              tableName: "posts",
            },
          },
          name: "t1",
        },
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
        tableReference: {
          type: "base_table_reference",
          tableName: "users",
        },
        condition: {
          type: "binary_operation",
          operator: "=",
          left: { type: "path", path: ["id"] },
          right: { type: "literal", value: 1 },
        },
      });
    });
    it("should parse a select statement with order by clause", () => {
      const sql = "SELECT * FROM users ORDER BY id, users.name DESC";
      const parser = new Parser(sql);
      const ast = parser.parse();
      expect(ast).toEqual({
        type: "select_statement",
        isAsterisk: true,
        selectElements: [],
        tableReference: {
          type: "base_table_reference",
          tableName: "users",
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
                path: ["users", "name"],
              },
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
        tableReference: {
          type: "base_table_reference",
          tableName: "users",
        },
        limit: {
          count: 10,
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
          { columnName: "id", dataType: "INTEGER" },
          { columnName: "name", dataType: "VARCHAR" },
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
  describe("expression", () => {
    it("should parse a number literal expression", () => {
      const sql = "SELECT 1 FROM t1";
      const parser = new Parser(sql);
      const ast = parser.parse() as SelectStatementAST;
      expect(ast.selectElements[0].expression).toEqual({
        type: "literal",
        value: 1,
      });
    });
    it("should parse a string literal expression", () => {
      const sql = "SELECT 'foo' FROM t1";
      const parser = new Parser(sql);
      const ast = parser.parse() as SelectStatementAST;
      expect(ast.selectElements[0].expression).toEqual({
        type: "literal",
        value: "foo",
      });
    });
    it("should parse a boolean literal expression", () => {
      const sql = "SELECT TRUE FROM t1";
      const parser = new Parser(sql);
      const ast = parser.parse() as SelectStatementAST;
      expect(ast.selectElements[0].expression).toEqual({
        type: "literal",
        value: true,
      });
    });
    it("should parse a null literal expression", () => {
      const sql = "SELECT NULL FROM t1";
      const parser = new Parser(sql);
      const ast = parser.parse() as SelectStatementAST;
      expect(ast.selectElements[0].expression).toEqual({
        type: "literal",
        value: null,
      });
    });
    it("should parse a simple add and subtract expression", () => {
      const sql = "SELECT 1 + 2 - 3 FROM t1";
      const parser = new Parser(sql);
      const ast = parser.parse() as SelectStatementAST;
      expect(ast.selectElements[0].expression).toEqual({
        type: "binary_operation",
        operator: "-",
        left: {
          type: "binary_operation",
          operator: "+",
          left: { type: "literal", value: 1 },
          right: { type: "literal", value: 2 },
        },
        right: { type: "literal", value: 3 },
      });
    });
    it("should parse a simple multiply expression", () => {
      const sql = "SELECT 1 * 2 FROM t1";
      const parser = new Parser(sql);
      const ast = parser.parse() as SelectStatementAST;
      expect(ast.selectElements[0].expression).toEqual({
        type: "binary_operation",
        operator: "*",
        left: { type: "literal", value: 1 },
        right: { type: "literal", value: 2 },
      });
    });
    it("should parse a complex arithmetic expression with paren", () => {
      const sql = "SELECT 1 + 2 * 3 * (4 + 5) FROM t1";
      const parser = new Parser(sql);
      const ast = parser.parse() as SelectStatementAST;
      expect(ast.selectElements[0].expression).toEqual({
        type: "binary_operation",
        operator: "+",
        left: { type: "literal", value: 1 },
        right: {
          type: "binary_operation",
          operator: "*",
          left: {
            type: "binary_operation",
            operator: "*",
            left: { type: "literal", value: 2 },
            right: { type: "literal", value: 3 },
          },
          right: {
            type: "binary_operation",
            operator: "+",
            left: { type: "literal", value: 4 },
            right: { type: "literal", value: 5 },
          },
        },
      });
    });
    it("should parse a simple equal expression", () => {
      const sql = "SELECT 1 * 3 = 2 + 1 FROM t1";
      const parser = new Parser(sql);
      const ast = parser.parse() as SelectStatementAST;
      expect(ast.selectElements[0].expression).toEqual({
        type: "binary_operation",
        operator: "=",
        left: {
          type: "binary_operation",
          operator: "*",
          left: { type: "literal", value: 1 },
          right: { type: "literal", value: 3 },
        },
        right: {
          type: "binary_operation",
          operator: "+",
          left: { type: "literal", value: 2 },
          right: { type: "literal", value: 1 },
        },
      });
    });
    it("should parse a simple logical and/or/not expression", () => {
      const sql = "SELECT 1 = 1 AND NOT 2 = 2 OR 3 = 3 FROM t1";
      const parser = new Parser(sql);
      const ast = parser.parse() as SelectStatementAST;
      expect(ast.selectElements[0].expression).toEqual({
        type: "binary_operation",
        operator: "OR",
        left: {
          type: "binary_operation",
          operator: "AND",
          left: {
            type: "binary_operation",
            operator: "=",
            left: { type: "literal", value: 1 },
            right: { type: "literal", value: 1 },
          },
          right: {
            type: "unary_operation",
            operator: "NOT",
            operand: {
              type: "binary_operation",
              operator: "=",
              left: { type: "literal", value: 2 },
              right: { type: "literal", value: 2 },
            },
          },
        },
        right: {
          type: "binary_operation",
          operator: "=",
          left: { type: "literal", value: 3 },
          right: { type: "literal", value: 3 },
        },
      });
    });
  });
});
