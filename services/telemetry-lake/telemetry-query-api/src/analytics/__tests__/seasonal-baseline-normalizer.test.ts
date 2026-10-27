import { describe, expect, it } from "vitest";
import { SeasonalBaselineNormalizer, type TimestampedValue } from "../seasonal-baseline-normalizer";

/**
 * Synthetic series: 4 weeks of hourly latency-style samples with a deliberate weekly/daily
 * seasonal shape (Mon-Fri 9am-5pm UTC "business hours" run +50ms hotter than nights/weekends)
 * plus a slow linear trend drift, so `fit()` has enough repetitions of every (day-of-week,
 * hour-of-day) bucket to build a trustworthy seasonal offset table.
 */
function buildSeasonalSeries(weeks: number, startIso: string): TimestampedValue[] {
  const series: TimestampedValue[] = [];
  const start = new Date(startIso);
  let hour = 0;
  for (let w = 0; w < weeks; w += 1) {
    for (let d = 0; d < 7; d += 1) {
      for (let h = 0; h < 24; h += 1) {
        const bucketMinute = new Date(start.getTime() + hour * 3_600_000);
        const dayOfWeek = bucketMinute.getUTCDay();
        const isBusinessHour = dayOfWeek >= 1 && dayOfWeek <= 5 && h >= 9 && h < 17;
        const seasonalBump = isBusinessHour ? 50 : 0;
        const trend = 100 + hour * 0.01;
        const smallNoise = Math.sin(hour * 1.7) * 2;
        series.push({ bucketMinute, value: trend + seasonalBump + smallNoise });
        hour += 1;
      }
    }
  }
  return series;
}

describe("SeasonalBaselineNormalizer", () => {
  it("throws when scoring or fitting without data", () => {
    const normalizer = new SeasonalBaselineNormalizer();
    expect(() => normalizer.fit([])).toThrow();
    expect(() => normalizer.decompose([{ bucketMinute: new Date(), value: 1 }])).toThrow();
  });

  it("assigns a small residual to a point that matches its expected seasonal shape", () => {
    // 2026-07-06 is a Monday (UTC), so this series starts cleanly on a week boundary.
    const history = buildSeasonalSeries(4, "2026-07-06T00:00:00Z");
    const normalizer = new SeasonalBaselineNormalizer();
    normalizer.fit(history);

    // A 5th-week Monday at 10am — squarely a "business hour" bucket the fit saw ~4 times.
    const week5MondayBusinessHour = new Date("2026-08-03T10:00:00Z");
    const expectedValue = 100 + (4 * 7 * 24 + 10) * 0.01 + 50; // trend + seasonal bump, no injected noise

    const [decomposed] = normalizer.decompose([{ bucketMinute: week5MondayBusinessHour, value: expectedValue }]);

    expect(Math.abs(decomposed.residualComponent)).toBeLessThan(10);
  });

  it("does not flag ordinary Sunday traffic as anomalous just because it differs from Monday's raw level", () => {
    const history = buildSeasonalSeries(4, "2026-07-06T00:00:00Z");
    const normalizer = new SeasonalBaselineNormalizer();
    normalizer.fit(history);

    // A perfectly ordinary Sunday (no business-hour bump expected) at the same wall-clock hour.
    const week5Sunday = new Date("2026-08-02T10:00:00Z");
    const expectedSundayValue = 100 + (3 * 7 * 24 + 6 * 24 + 10) * 0.01; // trend only, no bump, no noise

    const [decomposed] = normalizer.decompose([{ bucketMinute: week5Sunday, value: expectedSundayValue }]);

    expect(Math.abs(decomposed.residualComponent)).toBeLessThan(10);
  });

  it("gives a large residual to a value that matches Monday's raw level but occurs on a Sunday", () => {
    // This is the core "false positive" scenario this module exists to prevent from reaching the
    // anomaly detectors: the same raw number is completely ordinary on a Monday business-hour
    // bucket and wildly abnormal on a Sunday bucket where no seasonal bump is expected.
    const history = buildSeasonalSeries(4, "2026-07-06T00:00:00Z");
    const normalizer = new SeasonalBaselineNormalizer();
    normalizer.fit(history);

    const week5MondayBusinessHour = new Date("2026-08-03T10:00:00Z");
    const week5Sunday = new Date("2026-08-02T10:00:00Z");
    const mondayLevelValue = 100 + (4 * 7 * 24 + 10) * 0.01 + 50;

    const [mondayDecomposed] = normalizer.decompose([{ bucketMinute: week5MondayBusinessHour, value: mondayLevelValue }]);
    const [sundayDecomposed] = normalizer.decompose([{ bucketMinute: week5Sunday, value: mondayLevelValue }]);

    expect(Math.abs(mondayDecomposed.residualComponent)).toBeLessThan(10);
    expect(Math.abs(sundayDecomposed.residualComponent)).toBeGreaterThan(30);
  });

  it("flags a genuine spike even during a normally-hot business-hour bucket", () => {
    const history = buildSeasonalSeries(4, "2026-07-06T00:00:00Z");
    const normalizer = new SeasonalBaselineNormalizer();
    normalizer.fit(history);

    const week5MondayBusinessHour = new Date("2026-08-03T10:00:00Z");
    const genuineSpikeValue = 100 + (4 * 7 * 24 + 10) * 0.01 + 50 + 400; // seasonally-expected level + real regression

    const [decomposed] = normalizer.decompose([{ bucketMinute: week5MondayBusinessHour, value: genuineSpikeValue }]);

    expect(decomposed.residualComponent).toBeGreaterThan(300);
  });

  it("decompose() is pure with respect to the fitted model (repeated calls give identical results)", () => {
    const history = buildSeasonalSeries(4, "2026-07-06T00:00:00Z");
    const normalizer = new SeasonalBaselineNormalizer();
    normalizer.fit(history);

    const point: TimestampedValue = { bucketMinute: new Date("2026-08-03T10:00:00Z"), value: 175 };
    const first = normalizer.decompose([point]);
    const second = normalizer.decompose([point]);

    expect(first[0].residualComponent).toBeCloseTo(second[0].residualComponent, 10);
  });
});
