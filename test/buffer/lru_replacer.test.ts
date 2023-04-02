import { LRUReplacer } from "../../src/buffer/lru_replacer";

describe("LRUReplacer", () => {
  it("should work", () => {
    const replacer = new LRUReplacer();
    expect(replacer.size()).toBe(0);
    replacer.pin(0);
    replacer.pin(1);
    replacer.unpin(0);
    expect(replacer.size()).toBe(1);
    replacer.unpin(1);
    expect(replacer.size()).toBe(2);
    expect(replacer.victim()).toBe(0);
    expect(replacer.size()).toBe(1);
    replacer.pin(1);
    expect(replacer.size()).toBe(0);
    replacer.unpin(1);
    expect(replacer.victim()).toBe(1);
    expect(replacer.size()).toBe(0);
  });
});
