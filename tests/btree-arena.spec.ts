import { test, expect } from '@playwright/test';

/**
 * Joltrin Arena, B-tree challenge (/btree/). The engine is the real Go in-memory B-tree compiled
 * to WebAssembly, so these tests play the game the way a visitor does and check
 * what is measured, what is verified, and what happens when WebAssembly is
 * missing.
 */
async function ready(page: import('@playwright/test').Page) {
  await page.goto('/btree/', { waitUntil: 'domcontentloaded' });
  await expect(page.locator('#status')).toContainText('Engine ready', { timeout: 30_000 });
}

test.describe('Joltrin Arena B-tree challenge', () => {
  test('level 1 runs the real tree, verifies it, and unlocks level 2', async ({ page }) => {
    const errors: string[] = [];
    page.on('pageerror', (e) => errors.push(e.message));
    await ready(page);

    // Levels 2 to 4 start locked.
    await expect(page.locator('#progress button').nth(1)).toBeDisabled();

    await page.getByLabel('Between 100 thousand and 1 million a second').check();
    await page.getByRole('button', { name: /lock in my guess/i }).click();

    const result = page.locator('#result');
    await expect(result).toContainText('What it means', { timeout: 30_000 });
    await expect(result).toContainText('Every lookup found its key');
    await expect(result).toContainText('Tree holds 10000 items');
    // The setup that produced the numbers is on screen.
    await expect(result).toContainText('median of 3 trials');
    await expect(result).toContainText('seed 42');

    await expect(page.locator('#progress button').nth(1)).toBeEnabled();
    expect(errors).toEqual([]);
  });

  test('a full playthrough clears the arena and shows a score', async ({ page }) => {
    test.setTimeout(120_000);
    await ready(page);
    const picks = ['Under 100 thousand a second', 'Sorted order is faster', 'About 100 times faster', 'About the same time (under 2x)'];
    for (let i = 0; i < picks.length; i++) {
      await page.getByLabel(picks[i]).check();
      await page.getByRole('button', { name: /lock in my guess/i }).click();
      await expect(page.locator('#result')).toContainText('What it means', { timeout: 60_000 });
      if (i < picks.length - 1) await page.getByRole('button', { name: 'Next level' }).click();
    }
    await expect(page.locator('#result')).toContainText('Arena clear');
    await expect(page.locator('#result')).toContainText(/of 4 correctly/);
  });

  test('the sandbox runs a scenario and shows raw JSON; oversized input is not offered', async ({ page }) => {
    await ready(page);
    await page.locator('summary', { hasText: 'Open the sandbox' }).click();
    await page.locator('#sb-scenario').selectOption('range');
    await page.locator('#sb-n').selectOption('1000');
    await page.locator('#sb-run').click();
    await expect(page.locator('#sb-out')).toContainText('Range returned 10 keys', { timeout: 30_000 });
    await expect(page.locator('#raw')).toContainText('"scenario": "range"');
    const sizes = await page.locator('#sb-n option').allTextContents();
    expect(Math.max(...sizes.map(Number))).toBeLessThanOrEqual(100000);
  });

  test('the engine refuses an unsafe size even when called directly', async ({ page }) => {
    await ready(page);
    const out = await page.evaluate(() => {
      const f = (window as any).joltrinBtreeArena;
      return [f('basic', 10_000_000), f('basic', 5), f('nope', 1000), f('basic', 'x')].map((s: string) => JSON.parse(s).error);
    });
    for (const e of out) expect(typeof e).toBe('string');
    expect(out[0]).toContain('between 100 and 100000');
  });

  test('without WebAssembly the page explains itself instead of breaking', async ({ page }) => {
    await page.addInitScript(() => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      delete (window as any).WebAssembly;
    });
    await page.goto('/btree/', { waitUntil: 'domcontentloaded' });
    await expect(page.getByText('This page needs WebAssembly')).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Joltrin Arena' })).toBeVisible();
  });

  test('keyboard only: a guess can be chosen and submitted', async ({ page }) => {
    await ready(page);
    await page.locator('input[name="guess"]').first().focus();
    await page.keyboard.press('ArrowDown'); // moves within the radio group
    await page.keyboard.press('Tab');
    await expect(page.getByRole('button', { name: /lock in my guess/i })).toBeFocused();
    await page.keyboard.press('Enter');
    await expect(page.locator('#result')).toContainText('What it means', { timeout: 30_000 });
  });

  test('reduced motion: results show without animated bar growth', async ({ page }) => {
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await ready(page);
    await page.getByLabel('Between 100 thousand and 1 million a second').check();
    await page.getByRole('button', { name: /lock in my guess/i }).click();
    await expect(page.locator('#result')).toContainText('What it means', { timeout: 30_000 });
    const w = await page.locator('#result .bar-fill').first().evaluate((el) => (el as HTMLElement).style.width);
    expect(w).not.toBe('0%');
  });

  test('no horizontal scroll on a phone-sized screen', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 800 });
    await ready(page);
    const overflow = await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth);
    expect(overflow).toBeLessThanOrEqual(0);
  });
});
