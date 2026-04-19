const puppeteer = require('puppeteer');
(async () => {
  const browser = await puppeteer.launch();
  const page = await browser.newPage();
  page.on('console', msg => console.log('LOG:', msg.text()));
  page.on('pageerror', err => console.log('ERR:', err.message));
  
  await page.goto('http://localhost:8080/');
  
  await page.evaluate(() => {
    try {
        if (typeof showAddKnowledgeBase === 'function') {
            showAddKnowledgeBase();
            console.log('Called showAddKnowledgeBase');
        } else {
            console.log('showAddKnowledgeBase not available');
        }
    } catch(e) {
        console.log('LOG: Error:', e.message);
    }
  });
  
  await new Promise(r => setTimeout(r, 1000));
  try {
      const display = await page.$eval('#add-kb-modal', el => window.getComputedStyle(el).display);
      console.log('Modal display:', display);
  } catch(e) {
      console.log('Modal error:', e.message);
  }

  await browser.close();
})();
