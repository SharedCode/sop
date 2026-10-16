import { test, expect } from '@playwright/test';

const demoCommand = 'go run github.com/sharedcode/joltrin/v5/examples/verify_barrier@latest';

test.describe('First-run onboarding', () => {
  test('home page takes Go users directly to a runnable demo', async ({ page }) => {
    await page.goto('/', { waitUntil: 'domcontentloaded' });
    await page.locator('#start-here').getByRole('link', { name: /run the go example/i }).click();
    await expect(page).toHaveURL(/\/docs\/#local-demo$/);
    await expect(page.locator('#demo-command')).toHaveText(demoCommand);
    await expect(page.locator('#local-demo-guide')).toContainText('No real database is changed');
    await expect(page.locator('#local-demo-guide')).toContainText('command not found');
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  });

  test('copy command works by keyboard and announces success', async ({ page }) => {
    await page.addInitScript(() => {
      Object.defineProperty(navigator, 'clipboard', { value: {
        writeText: async (value: string) => { (window as Window & { copiedCommand?: string }).copiedCommand = value; },
      } });
    });
    await page.goto('/docs/#local-demo');
    const button = page.getByRole('button', { name: 'Copy Go demo command' });
    await button.focus();
    await page.keyboard.press('Enter');
    await expect(page.locator('#local-demo-guide').getByRole('status')).toHaveText('Go demo command copied.');
    expect(await page.evaluate(() => (window as Window & { copiedCommand?: string }).copiedCommand)).toBe(demoCommand);
    await expect(button).toBeFocused();
  });

  test('clipboard rejection selects the command for manual copying', async ({ page }) => {
    await page.addInitScript(() => {
      Object.defineProperty(navigator, 'clipboard', { value: {
        writeText: async () => { throw new Error('Clipboard denied'); },
      } });
    });
    await page.goto('/docs/#mcp');
    await page.getByRole('button', { name: 'Copy MCP install command' }).click();
    await expect(page.locator('#mcp-install-guide').getByRole('status')).toHaveText('Copy unavailable. The command is selected; copy it manually.');
    expect(await page.evaluate(() => window.getSelection()?.toString())).toBe(
      'curl -fsSL https://raw.githubusercontent.com/SharedCode/joltrin/master/scripts/install.sh | sh',
    );
    await expect(page.locator('#mcp-install-guide').getByRole('link', { name: 'Review the install script' })).toHaveAttribute('href', /scripts\/install\.sh$/);
  });

  test('commands and recovery instructions remain available without JavaScript', async ({ browser }) => {
    const context = await browser.newContext({ javaScriptEnabled: false });
    const page = await context.newPage();
    await page.goto('/docs/#local-demo');
    await expect(page.locator('#demo-command')).toHaveText(demoCommand);
    await expect(page.getByRole('button', { name: 'Copy Go demo command' })).toBeHidden();
    await expect(page.getByRole('link', { name: 'Install Go' })).toHaveAttribute('href', 'https://go.dev/doc/install');
    await context.close();
  });
});
