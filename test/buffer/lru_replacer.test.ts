import { LRUReplacer } from "../../src/buffer/lru_replacer";

describe("LRUReplacer", () => {
  it("Scenario", () => {
    const replacer = new LRUReplacer();
    expect(replacer.size()).toBe(0);

    // Scenario: unpin six elements, i.e. add them to the replacer.
    replacer.unpin(0);
    replacer.unpin(1);
    replacer.unpin(2);
    replacer.unpin(3);
    replacer.unpin(4);
    replacer.unpin(5);
    replacer.unpin(0);
    expect(replacer.size()).toBe(6);

    // Scenario: get three victims from the lru.
    const frameId0 = replacer.victim();
    expect(frameId0).toBe(0);
    expect(replacer.size()).toBe(5);
    const frameId1 = replacer.victim();
    expect(frameId1).toBe(1);
    expect(replacer.size()).toBe(4);
    const frameId2 = replacer.victim();
    expect(frameId2).toBe(2);

    // Scenario: pin elements in the replacer.
    // Note that 2 has already been victimized, so pinning 2 should have no effect.
    replacer.pin(2);
    replacer.pin(3);
    expect(replacer.size()).toBe(2);

    // Scenario: unpin 3. We expect that the reference bit of 3 will be set to 1
    replacer.unpin(3);
    expect(replacer.size()).toBe(3);

    // Scenario: continue looking for victims. We expect these victims.
    const frameId4 = replacer.victim();
    expect(frameId4).toBe(4);
    expect(replacer.size()).toBe(2);
    const frameId5 = replacer.victim();
    expect(frameId5).toBe(5);
    expect(replacer.size()).toBe(1);
    const frameId3 = replacer.victim();
    expect(frameId3).toBe(3);
    expect(replacer.size()).toBe(0);
  });
});
