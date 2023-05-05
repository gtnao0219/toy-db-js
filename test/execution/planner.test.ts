import { BoundStatement } from "../../src/binder/bound";
import { Column } from "../../src/catalog/column";
import { Schema } from "../../src/catalog/schema";
import { plan } from "../../src/execution/planner";
import { Type } from "../../src/type/type";

describe("planner", () => {
  describe("plan", () => {
    it("should plan a select query", () => {
      const statement: BoundStatement = {
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
        orderBy: {
          sortKeys: [
            {
              expression: {
                type: "path",
                path: ["_id"],
              },
              direction: "ASC",
            },
          ],
        },
        limit: {
          count: {
            type: "literal",
            value: 1,
          },
        },
      };
      const planNode = plan(statement);
      expect(planNode).toEqual({
        type: "limit",
        count: {
          type: "literal",
          value: 1,
        },
        outputSchema: new Schema([
          new Column("_id", Type.INTEGER),
          new Column("name", Type.VARCHAR),
        ]),
        child: {
          type: "sort",
          sortKeys: [
            {
              expression: {
                type: "path",
                tupleIndex: 0,
                columnIndex: 0,
                dataType: Type.INTEGER,
              },
              direction: "ASC",
            },
          ],
          outputSchema: new Schema([
            new Column("_id", Type.INTEGER),
            new Column("name", Type.VARCHAR),
          ]),
          child: {
            type: "project",
            selectElements: [
              {
                expression: {
                  type: "path",
                  tupleIndex: 0,
                  columnIndex: 0,
                  dataType: Type.INTEGER,
                },
                alias: "_id",
              },
              {
                expression: {
                  type: "path",
                  tupleIndex: 0,
                  columnIndex: 1,
                  dataType: Type.VARCHAR,
                },
              },
            ],
            outputSchema: new Schema([
              new Column("_id", Type.INTEGER),
              new Column("name", Type.VARCHAR),
            ]),
            child: {
              type: "filter",
              condition: {
                type: "binary_operation",
                operator: "=",
                left: {
                  type: "path",
                  tupleIndex: 0,
                  columnIndex: 0,
                  dataType: Type.INTEGER,
                },
                right: {
                  type: "literal",
                  value: 1,
                },
              },
              outputSchema: new Schema([
                new Column("bar.id", Type.INTEGER),
                new Column("bar.name", Type.VARCHAR),
              ]),
              child: {
                type: "seq_scan",
                tableOid: 1,
                outputSchema: new Schema([
                  new Column("bar.id", Type.INTEGER),
                  new Column("bar.name", Type.VARCHAR),
                ]),
              },
            },
          },
        },
      });
    });
  });
});
