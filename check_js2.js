const fs = require('fs');
const code = fs.readFileSync('tools/httpserver/templates/scripts_part01.html', 'utf8');
const match = code.match(/<script>([\s\S]*?)<\/script>/);
if (match) {
    const { execSync } = require('child_process');
    fs.writeFileSync('temp.js', match[1]);
    try {
        execSync('node -c temp.js', { stdio: 'inherit' });
        console.log("Syntax OK");
    } catch(e) {
        console.log("Syntax Error Detected");
    }
}
