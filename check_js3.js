const puppeteer = require('puppeteer');
(async () => {
  const browser = await puppeteer.launch();
  const page = await browser.newPage();
  page.on('console', msg => console.log('LOG:', msg.text()));
  page.on('pageerror', err => console.log('ERR:', err.message));
  
  await page.goto('http://localhost:8080/');
  
  // Wait a second for fetch to finish
  await new Promise(r => setTimeout(r, 1000));
  
  const display = await page.$eval('#database-section', el => window.getComputedStyle(el).display);
  console.log('database-section display:', display);

  const displaySelect = await page.$eval('#database-select', el => window.getComputedStyle(el).display);
  console.log('database-select display:', displaySelect);

  const content = await page.content();
  const index = content.indexOf('<div id="database-section"');
  console.log('database-section snippet:');
  console.log(content.substring(index, index + 300));
  
  await browser.close();
})();
