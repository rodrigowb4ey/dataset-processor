import { describe, expect, it } from "vitest";

import { normalizeErrorDetail } from "./api";

describe("normalizeErrorDetail", () => {
  it("returns plain detail when backend responds with a string", () => {
    expect(normalizeErrorDetail("Dataset not found.")).toBe("Dataset not found.");
  });

  it("compacts validation errors into a single message", () => {
    const detail = [
      {
        loc: ["body", "name"],
        msg: "field required",
      },
      {
        loc: ["body", "file"],
        msg: "invalid content type",
      },
    ];

    expect(normalizeErrorDetail(detail)).toBe(
      "body.name: field required; body.file: invalid content type",
    );
  });

  it("returns generic fallback when detail payload shape is unknown", () => {
    expect(normalizeErrorDetail({ nope: true })).toBe("Request failed. Please try again.");
  });
});
