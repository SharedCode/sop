import { Page, expect, ConsoleMessage } from '@playwright/test';

/**
 * Network & Console Purity Monitor
 *
 * Ensures zero uncaught runtime exceptions, zero unhandled Promise rejections,
 * and zero failed network requests (HTTP >= 400) for all visited pages and assets.
 */
export class NetworkConsoleGuard {
  private page: Page;
  private consoleErrors: string[] = [];
  private uncaughtExceptions: string[] = [];
  private failedRequests: { url: string; status: number; statusText: string }[] = [];
  private allowedFailedUrls: RegExp[] = [];
  private allowedConsoleErrors: RegExp[] = [];

  constructor(page: Page) {
    this.page = page;
    this.setupListeners();
  }

  /**
   * Allow specific URLs to fail (e.g. testing intentional network fallbacks or external third-party badges)
   */
  public allowFailedUrl(pattern: RegExp): this {
    this.allowedFailedUrls.push(pattern);
    return this;
  }

  /**
   * Allow specific console warnings/errors matching regex pattern if expected
   */
  public allowConsoleError(pattern: RegExp): this {
    this.allowedConsoleErrors.push(pattern);
    return this;
  }

  private setupListeners(): void {
    // 1. Capture Browser Console Errors
    this.page.on('console', (msg: ConsoleMessage) => {
      if (msg.type() === 'error') {
        const text = msg.text();
        const isAllowed = this.allowedConsoleErrors.some((re) => re.test(text));
        if (!isAllowed) {
          this.consoleErrors.push(`[Console Error] ${text}`);
        }
      }
    });

    // 2. Capture Uncaught Page Exceptions & Unhandled Promise Rejections
    this.page.on('pageerror', (err: Error) => {
      const message = err.stack || err.message;
      const isAllowed = this.allowedConsoleErrors.some((re) => re.test(message));
      if (!isAllowed) {
        this.uncaughtExceptions.push(`[Uncaught Page Error] ${message}`);
      }
    });

    // 3. Monitor Network Responses for HTTP >= 400 failures
    this.page.on('response', (response) => {
      const status = response.status();
      const url = response.url();

      if (status >= 400) {
        const isAllowed = this.allowedFailedUrls.some((re) => re.test(url));
        if (!isAllowed) {
          this.failedRequests.push({
            url,
            status,
            statusText: response.statusText(),
          });
        }
      }
    });
  }

  /**
   * Assert that no console errors, uncaught exceptions, or failed network requests occurred
   */
  public assertPurity(contextLabel = 'Page Purity'): void {
    const failures: string[] = [];

    if (this.uncaughtExceptions.length > 0) {
      failures.push(
        `Uncaught exceptions detected in ${contextLabel}:\n` +
          this.uncaughtExceptions.join('\n')
      );
    }

    if (this.consoleErrors.length > 0) {
      failures.push(
        `Console errors logged in ${contextLabel}:\n` +
          this.consoleErrors.join('\n')
      );
    }

    if (this.failedRequests.length > 0) {
      const formatted = this.failedRequests
        .map((r) => `  - HTTP ${r.status} (${r.statusText}): ${r.url}`)
        .join('\n');
      failures.push(`Failed network requests (HTTP >= 400) in ${contextLabel}:\n${formatted}`);
    }

    expect(failures, failures.join('\n\n')).toEqual([]);
  }

  /**
   * Reset captured collections (useful when navigating between multiple pages)
   */
  public reset(): void {
    this.consoleErrors = [];
    this.uncaughtExceptions = [];
    this.failedRequests = [];
  }

  public getFailedRequests() {
    return [...this.failedRequests];
  }

  public getConsoleErrors() {
    return [...this.consoleErrors];
  }

  public getUncaughtExceptions() {
    return [...this.uncaughtExceptions];
  }
}
