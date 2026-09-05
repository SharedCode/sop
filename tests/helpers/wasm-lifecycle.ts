import { Page, expect } from '@playwright/test';

/**
 * Web-First WASM Lifecycle & Rendering Synchronization
 *
 * Replaces brittle page.waitForTimeout sleeps with deterministic DOM state
 * and WebAssembly execution boundary listeners.
 */
export async function waitForWasmReady(page: Page): Promise<void> {
  // Wait for the loader overlay to hide (opacity-0 class added upon WASM instantiation)
  const overlay = page.locator('#wasm-loader-overlay');
  if (await overlay.count() > 0) {
    await expect(overlay).toHaveClass(/opacity-0/, { timeout: 25_000 });
  }

  // Wait for status text to confirm active kernel status
  const statusText = page.locator('#wasm-status-text');
  if (await statusText.count() > 0) {
    await expect(statusText).toHaveText(/ACTIVE/i, {
      timeout: 25_000,
    });
  }

  // Ensure OPFS status badge has resolved away from initial "checking..." state
  const opfsBadge = page.locator('#opfs-status-text');
  if (await opfsBadge.count() > 0) {
    await expect(opfsBadge).not.toHaveText('checking...', { timeout: 15_000 });
  }
}

/**
 * Ensures Canvas animation loop has initialized and rendered frames
 */
export async function waitForCanvasRendered(page: Page, canvasSelector = 'canvas'): Promise<void> {
  const canvas = page.locator(canvasSelector).first();
  await expect(canvas).toBeVisible({ timeout: 15_000 });

  // Verify canvas dimensions are valid (> 0)
  const box = await canvas.boundingBox();
  expect(box).not.toBeNull();
  expect(box!.width).toBeGreaterThan(100);
  expect(box!.height).toBeGreaterThan(100);

  // Wait for at least 2 animation frames to confirm continuous render loop
  await page.evaluate(() => {
    return new Promise<void>((resolve) => {
      requestAnimationFrame(() => {
        requestAnimationFrame(() => {
          resolve();
        });
      });
    });
  });
}
