const puppeteer = require('puppeteer');
(async () => {
  const browser = await puppeteer.launch();
  const page = await browser.newPage();
  page.on('console', msg => console.log('LOG:', msg.text()));
  page.on('pageerror', err => console.log('ERR:', err.message));
  
  await page.goto('http://localhost:8080/');
  await page.waitForSelector('#db-options-section');
  
  const display = await page.$eval('#db-options-section', el => window.getComputedStyle(el).display);
  console.log('db-options-section display:', display);

  await browser.close();
})();
