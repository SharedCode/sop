import { Page, expect } from '@playwright/test';

/**
 * Cumulative Layout Shift (CLS) Observer Helper
 *
 * Attaches a PerformanceObserver to track unexpected layout shifts
 * adhering to Google Core Web Vitals specifications.
 */
export class LayoutShiftObserver {
  private page: Page;

  constructor(page: Page) {
    this.page = page;
  }

  /**
   * Initializes PerformanceObserver before page load/actions
   */
  public async initialize(): Promise<void> {
    await this.page.addInitScript(() => {
      window.__clsScore = 0;
      window.__layoutShifts = [];

      try {
        const observer = new PerformanceObserver((entryList) => {
          for (const entry of entryList.getEntries()) {
            // Only count shifts without recent user input (unprompted shifts)
            const shift = entry as PerformanceEntry & {
              value: number;
              hadRecentInput: boolean;
              sources?: unknown[];
            };
            if (!shift.hadRecentInput) {
              window.__clsScore = (window.__clsScore || 0) + shift.value;
              window.__layoutShifts.push({
                value: shift.value,
                startTime: shift.startTime,
              });
            }
          }
        });

        observer.observe({ type: 'layout-shift', buffered: true });
      } catch {
        // Fallback gracefully if PerformanceObserver is unsupported in specific webview
      }
    });
  }

  /**
   * Retrieve current CLS score
   */
  public async getScore(): Promise<number> {
    return await this.page.evaluate(() => {
      return (window as unknown as { __clsScore?: number }).__clsScore || 0;
    });
  }

  /**
   * Asserts that the cumulative layout shift stays below maximum threshold (default: 0.1)
   */
  public async assertMinimalCLS(threshold = 0.1): Promise<void> {
    const score = await this.getScore();
    expect(
      score,
      `Observed Cumulative Layout Shift (${score.toFixed(4)}) exceeded allowable threshold of ${threshold}`
    ).toBeLessThanOrEqual(threshold);
  }
}

declare global {
  interface Window {
    __clsScore?: number;
    __layoutShifts?: { value: number; startTime: number }[];
  }
}
