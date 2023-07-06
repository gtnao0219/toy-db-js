import { Column } from "../../../src/catalog/column";
import { Schema } from "../../../src/catalog/schema";
import { Tuple } from "../../../src/storage/table/tuple";
import { BooleanValue } from "../../../src/type/boolean_value";
import { IntegerValue } from "../../../src/type/integer_value";
import { Type } from "../../../src/type/type";
import { VarcharValue } from "../../../src/type/varchar_value";

describe("Tuple", () => {
  describe("deserialize", () => {
    test("all types", () => {
      const schema = new Schema([
        new Column("integer", Type.INTEGER),
        new Column("varchar", Type.VARCHAR),
        new Column("boolean", Type.BOOLEAN),
      ]);
      const integerValueBuffer = new Uint8Array([
        // 19088743
        0x01, 0x23, 0x45, 0x67,
      ]);
      const varcharValueBuffer = new Uint8Array([
        // length
        0x00, 0x00, 0x00, 0x05,
        // abcde
        0x61, 0x62, 0x63, 0x64, 0x65,
      ]);
      const booleanValueBuffer = new Uint8Array([0x01]);
      const buffer = new Uint8Array([
        ...integerValueBuffer,
        ...varcharValueBuffer,
        ...booleanValueBuffer,
      ]);
      const tuple = Tuple.deserialize(buffer.buffer, schema);
      expect(tuple.values[0].value).toBe(19088743);
      expect(tuple.values[1].value).toBe("abcde");
      expect(tuple.values[2].value).toBe(true);
    });
  });
  describe("serialize", () => {
    test("all types", () => {
      const schema = new Schema([
        new Column("integer", Type.INTEGER),
        new Column("varchar", Type.VARCHAR),
        new Column("boolean", Type.BOOLEAN),
      ]);
      const integerValue = new IntegerValue(19088743);
      const varcharValue = new VarcharValue("abcde");
      const booleanValue = new BooleanValue(true);
      const values = [integerValue, varcharValue, booleanValue];
      const tuple = new Tuple(schema, values);
      const buffer = tuple.serialize();
      const integerValueBuffer = new Uint8Array([
        // 19088743
        0x01, 0x23, 0x45, 0x67,
      ]);
      const varcharValueBuffer = new Uint8Array([
        // length
        0x00, 0x00, 0x00, 0x05,
        // abcde
        0x61, 0x62, 0x63, 0x64, 0x65,
      ]);
      const booleanValueBuffer = new Uint8Array([0x01]);
      const expectedBuffer = new Uint8Array([
        ...integerValueBuffer,
        ...varcharValueBuffer,
        ...booleanValueBuffer,
      ]);
      expect(buffer).toEqual(expectedBuffer.buffer);
    });
  });
});
