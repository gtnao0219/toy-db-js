import { Column } from "../../../src/catalog/column";
import { Schema } from "../../../src/catalog/schema";
import {
  TablePage,
  TablePageDeserializer,
} from "../../../src/storage/page/table_page";
import { Tuple } from "../../../src/storage/table/tuple";
import { IntegerValue } from "../../../src/type/integer_value";
import { Type } from "../../../src/type/type";
import { VarcharValue } from "../../../src/type/varchar_value";

describe("TablePage", () => {
  it("should serialize and deserialize", async () => {
    const schema = new Schema([
      new Column("id", Type.INTEGER),
      new Column("name", Type.VARCHAR),
    ]);
    const tuples = [
      new Tuple(schema, [new IntegerValue(1), new VarcharValue("a")]),
      new Tuple(schema, [new IntegerValue(2), new VarcharValue("b")]),
    ];
    const page = new TablePage(1, 2, schema, 3, 4, tuples);
    const buffer = page.serialize();
    const newPage = await new TablePageDeserializer(schema).deserialize(buffer);
    expect(newPage.pageId).toBe(1);
    expect(newPage.oid).toBe(2);
    expect(newPage.schema).toEqual(schema);
    expect(newPage.lsn).toBe(3);
    expect(newPage.nextPageId).toBe(4);
    expect(newPage.tuples.length).toBe(2);
    expect(newPage.tuples[0]!.values[0].value).toBe(1);
    expect(newPage.tuples[0]!.values[1].value).toBe("a");
    expect(newPage.tuples[1]!.values[0].value).toBe(2);
    expect(newPage.tuples[1]!.values[1].value).toBe("b");
  });
});
