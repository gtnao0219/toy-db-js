import { Token } from "./token";

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
      switch (str.toUpperCase()) {
        case "CREATE":
          tokens.push({ type: "keyword", value: "CREATE" });
          break;
        case "TABLE":
          tokens.push({ type: "keyword", value: "TABLE" });
          break;
        case "INSERT":
          tokens.push({ type: "keyword", value: "INSERT" });
          break;
        case "INTO":
          tokens.push({ type: "keyword", value: "INTO" });
          break;
        case "VALUES":
          tokens.push({ type: "keyword", value: "VALUES" });
          break;
        case "DELETE":
          tokens.push({ type: "keyword", value: "DELETE" });
          break;
        case "UPDATE":
          tokens.push({ type: "keyword", value: "UPDATE" });
          break;
        case "SET":
          tokens.push({ type: "keyword", value: "SET" });
          break;
        case "SELECT":
          tokens.push({ type: "keyword", value: "SELECT" });
          break;
        case "FROM":
          tokens.push({ type: "keyword", value: "FROM" });
          break;
        case "WHERE":
          tokens.push({ type: "keyword", value: "WHERE" });
          break;
        case "INTEGER":
          tokens.push({ type: "keyword", value: "INTEGER" });
          break;
        case "VARCHAR":
          tokens.push({ type: "keyword", value: "VARCHAR" });
          break;
        case "BOOLEAN":
          tokens.push({ type: "keyword", value: "BOOLEAN" });
          break;
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
  return tokens;
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
