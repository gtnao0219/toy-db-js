import { tokenize } from "../../src/parser/lexer";
import { KEYWORDS } from "../../src/parser/token";

describe("tokenize", () => {
  it("should tokenize null literal", () => {
    expect(tokenize("null NULL")).toEqual([
      { type: "literal", value: { type: "null", value: null } },
      { type: "literal", value: { type: "null", value: null } },
    ]);
  });
  it("should tokenize boolean literal", () => {
    expect(tokenize("true TRUE")).toEqual([
      { type: "literal", value: { type: "boolean", value: true } },
      { type: "literal", value: { type: "boolean", value: true } },
    ]);
    expect(tokenize("false FALSE")).toEqual([
      { type: "literal", value: { type: "boolean", value: false } },
      { type: "literal", value: { type: "boolean", value: false } },
    ]);
  });
  it("should tokenize number literal", () => {
    expect(tokenize("1 12")).toEqual([
      { type: "literal", value: { type: "number", value: 1 } },
      { type: "literal", value: { type: "number", value: 12 } },
    ]);
  });
  it("should tokenize string literal", () => {
    expect(tokenize("'a' 'foo'")).toEqual([
      { type: "literal", value: { type: "string", value: "a" } },
      { type: "literal", value: { type: "string", value: "foo" } },
    ]);
  });
  it("should tokenize identifier", () => {
    expect(tokenize("a foo")).toEqual([
      { type: "identifier", value: "a" },
      { type: "identifier", value: "foo" },
    ]);
  });
  it("should tokenize symbol", () => {
    expect(tokenize("*;,.()=<><<=>>=+-")).toEqual([
      { type: "asterisk" },
      { type: "semicolon" },
      { type: "comma" },
      { type: "dot" },
      { type: "left_paren" },
      { type: "right_paren" },
      { type: "equal" },
      { type: "not_equal" },
      { type: "less_than" },
      { type: "less_than_equal" },
      { type: "greater_than" },
      { type: "greater_than_equal" },
      { type: "plus" },
      { type: "minus" },
    ]);
  });
  it("should tokenize keyword", () => {
    const str =
      KEYWORDS.join(" ") + " " + KEYWORDS.map((k) => k.toLowerCase()).join(" ");
    expect(tokenize(str)).toEqual(
      KEYWORDS.map((k) => ({ type: "keyword", value: k })).concat(
        KEYWORDS.map((k) => ({ type: "keyword", value: k }))
      )
    );
  });
});