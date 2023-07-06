import { VarcharValue } from "../../src/type/varchar_value";

describe("VarcharValue", () => {
  describe("deserialize", () => {
    test("empty", () => {
      const buffer = new Uint8Array([
        0x01, 0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00,
      ]).buffer;
      const value = VarcharValue.deserialize(buffer, 4);
      expect(value.value).toBe("");
    });
    test("ascii", () => {
      const buffer = new Uint8Array([
        0x01, 0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0x04, 0x61, 0x62, 0x63, 0x64,
      ]).buffer;
      const value = VarcharValue.deserialize(buffer, 4);
      expect(value.value).toBe("abcd");
    });
    test("multi byte", () => {
      const buffer = new Uint8Array([
        0x01, 0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0x0f, 0xe3, 0x81, 0x82, 0xe3,
        0x81, 0x84, 0xe3, 0x81, 0x86, 0xe3, 0x81, 0x88, 0xe3, 0x81, 0x8a,
      ]).buffer;
      const value = VarcharValue.deserialize(buffer, 4);
      expect(value.value).toBe("あいうえお");
    });
  });
  describe("serialize", () => {
    test("empty", () => {
      const value = new VarcharValue("");
      const buffer = new Uint8Array([0x00, 0x00, 0x00, 0x00]).buffer;
      expect(value.serialize()).toEqual(buffer);
    });
    test("ascii", () => {
      const value = new VarcharValue("abcd");
      const buffer = value.serialize();
      expect(new Uint8Array(buffer)).toEqual(
        new Uint8Array([0x00, 0x00, 0x00, 0x04, 0x61, 0x62, 0x63, 0x64])
      );
    });
    test("multi byte", () => {
      const value = new VarcharValue("あいうえお");
      const buffer = value.serialize();
      expect(new Uint8Array(buffer)).toEqual(
        new Uint8Array([
          0x00, 0x00, 0x00, 0x0f, 0xe3, 0x81, 0x82, 0xe3, 0x81, 0x84, 0xe3,
          0x81, 0x86, 0xe3, 0x81, 0x88, 0xe3, 0x81, 0x8a,
        ])
      );
    });
  });
});
