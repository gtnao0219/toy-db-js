import { KEYWORDS, Token } from "./token";

export function tokenize(input: string): Token[] {
  const tokens: Token[] = [];
  let current = 0;
  while (current < input.length) {
    let char = input[current];
    if (is_whitespace(char)) {
      current++;
      continue;
    }
    if (is_alpha(char) || char === "_") {
      let str = "";
      while (is_alpha(char) || is_digit(char) || char === "_") {
        str += char;
        if (current + 1 >= input.length) {
          current++;
          break;
        }
        char = input[++current];
      }
      const keyword = KEYWORDS.find((keyword) => keyword === str.toUpperCase());
      if (keyword != null) {
        tokens.push({ type: "keyword", value: keyword });
        continue;
      }
      switch (str.toUpperCase()) {
        case "TRUE":
          tokens.push({
            type: "literal",
            value: { type: "boolean", value: true },
          });
          break;
        case "FALSE":
          tokens.push({
            type: "literal",
            value: { type: "boolean", value: false },
          });
          break;
        case "NULL":
          tokens.push({
            type: "literal",
            value: { type: "null", value: null },
          });
          break;
        default:
          tokens.push({ type: "identifier", value: str });
      }
      continue;
    }
    if (char === "*") {
      tokens.push({ type: "asterisk" });
      current++;
      continue;
    }
    if (char === ";") {
      tokens.push({ type: "semicolon" });
      current++;
      continue;
    }
    if (char === ",") {
      tokens.push({ type: "comma" });
      current++;
      continue;
    }
    if (char === ".") {
      tokens.push({ type: "dot" });
      current++;
      continue;
    }
    if (char === "(") {
      tokens.push({ type: "left_paren" });
      current++;
      continue;
    }
    if (char === ")") {
      tokens.push({ type: "right_paren" });
      current++;
      continue;
    }
    if (char === "=") {
      tokens.push({ type: "equal" });
      current++;
      continue;
    }
    if (char === "<") {
      if (input[current + 1] === "=") {
        tokens.push({ type: "less_than_equal" });
        current += 2;
        continue;
      }
      if (input[current + 1] === ">") {
        tokens.push({ type: "not_equal" });
        current += 2;
        continue;
      }
      tokens.push({ type: "less_than" });
      current++;
      continue;
    }
    if (char === ">") {
      if (input[current + 1] === "=") {
        tokens.push({ type: "greater_than_equal" });
        current += 2;
        continue;
      }
      tokens.push({ type: "greater_than" });
      current++;
      continue;
    }
    if (char === "+") {
      tokens.push({ type: "plus" });
      current++;
      continue;
    }
    if (char === "-") {
      tokens.push({ type: "minus" });
      current++;
      continue;
    }
    if (is_digit(char)) {
      let str = "";
      while (is_digit(char)) {
        str += char;
        if (current + 1 >= input.length) {
          current++;
          break;
        }
        char = input[++current];
      }
      const value = parseInt(str);
      if (isNaN(value)) {
        throw new Error("Invalid number");
      } else {
        tokens.push({
          type: "literal",
          value: {
            type: "number",
            value,
          },
        });
      }
      continue;
    }
    if (char === "'") {
      let str = "";
      if (current + 1 >= input.length) {
        throw new Error("Unterminated string");
      }
      char = input[++current];
      while (char !== "'") {
        str += char;
        if (current + 1 >= input.length) {
          throw new Error("Unterminated string");
        }
        char = input[++current];
      }
      tokens.push({
        type: "literal",
        value: {
          type: "string",
          value: str,
        },
      });
      current++;
      continue;
    }
    throw new Error(`Unexpected character: ${char}`);
  }
  return tokens.concat({ type: "eof" });
}

function is_whitespace(char: string): boolean {
  return /^\s$/.test(char);
}
function is_alpha(char: string): boolean {
  return /^[A-z]$/i.test(char);
}
function is_digit(char: string): boolean {
  return /^[0-9]$/.test(char);
}
