import { BooleanValue } from "../../src/type/boolean_value";

describe("BooleanValue", () => {
  describe("deserialize", () => {
    test("true", () => {
      const buffer = new Uint8Array([0x00, 0x00, 0x00, 0x00, 0x01]).buffer;
      const value = BooleanValue.deserialize(buffer, 4);
      expect(value.value).toBe(true);
    });
    test("false", () => {
      const buffer = new Uint8Array([0x01, 0x01, 0x01, 0x01, 0x00]).buffer;
      const value = BooleanValue.deserialize(buffer, 4);
      expect(value.value).toBe(false);
    });
    test("invalid", () => {
      const buffer = new Uint8Array([0x00, 0x00, 0x00, 0x00, 0x02]).buffer;
      expect(() => {
        BooleanValue.deserialize(buffer, 4);
      }).toThrowError();
    });
  });
  describe("serialize", () => {
    test("true", () => {
      const value = new BooleanValue(true);
      const buffer = value.serialize();
      expect(new Uint8Array(buffer)).toEqual(new Uint8Array([0x01]));
    });
    test("false", () => {
      const value = new BooleanValue(false);
      const buffer = value.serialize();
      expect(new Uint8Array(buffer)).toEqual(new Uint8Array([0x00]));
    });
  });
});
