import { IntegerValue } from "../../src/type/integer_value";

describe("IntegerValue", () => {
  describe("deserialize", () => {
    test("0", () => {
      const buffer = new Uint8Array([
        0x01, 0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00,
      ]).buffer;
      const value = IntegerValue.deserialize(buffer, 4);
      expect(value.value).toBe(0);
    });
    test("max", () => {
      const buffer = new Uint8Array([
        0x01, 0x01, 0x01, 0x01, 0x7f, 0xff, 0xff, 0xff,
      ]).buffer;
      const value = IntegerValue.deserialize(buffer, 4);
      expect(value.value).toBe(2147483647);
    });
    test("min", () => {
      const buffer = new Uint8Array([
        0x01, 0x01, 0x01, 0x01, 0x80, 0x00, 0x00, 0x01,
      ]).buffer;
      const value = IntegerValue.deserialize(buffer, 4);
      expect(value.value).toBe(-2147483647);
    });
  });
  describe("serialize", () => {
    test("0", () => {
      const value = new IntegerValue(0);
      const buffer = new Uint8Array([0x00, 0x00, 0x00, 0x00]).buffer;
      expect(value.serialize()).toEqual(buffer);
    });
    test("max", () => {
      const value = new IntegerValue(2147483647);
      const buffer = new Uint8Array([0x7f, 0xff, 0xff, 0xff]).buffer;
      expect(value.serialize()).toEqual(buffer);
    });
    test("min", () => {
      const value = new IntegerValue(-2147483647);
      const buffer = new Uint8Array([0x80, 0x00, 0x00, 0x01]).buffer;
      expect(value.serialize()).toEqual(buffer);
    });
  });
});
