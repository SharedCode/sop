const fs = require('fs');
const code = fs.readFileSync('tools/httpserver/templates/scripts_part01.html', 'utf8');
const match = code.match(/<script>([\s\S]*?)<\/script>/);
if (match) {
    try {
        new Function(match[1]);
        console.log("Syntax OK");
    } catch(e) {
        console.log("Syntax Error:", e);
    }
}
