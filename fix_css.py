import re

with open('tools/httpserver/templates/styles.html', 'r', encoding='utf-8') as f:
    content = f.read()

pattern = re.compile(r'@media \(max-width: 768px\) \{.+?#add-store-modal \.modal-content \{ width: 95%; \}\n        \}', re.DOTALL)

replacement = """@media (max-width: 768px) {
            #sidebar, #resizer, #main, #detail-panel { display: none !important; }
            
            /* Full screen chat on mobile for immersive experience */
            .chat-widget { 
                position: fixed !important;
                top: 0 !important;
                left: 0 !important;
                right: 0 !important;
                bottom: 0 !important;
                width: 100% !important;
                height: 100% !important;
                margin: 0 !important;
                border: none !important;
                border-radius: 0 !important;
                z-index: 1500;
                display: flex !important; /* Always display */
                flex                flex                bo                flex                flex                bo                flea ha                flex      padding-top: env(safe-area-inset-top) !important;
                padding-bottom: env(safe-area-inset-bottom) !important;
                                                    ft) !important;
                padding-right: env(safe-area-inset-right) !important;
                                                                                po                          t                                          ntr                                fle                                  nt                                   px                                   und                                      -color); border-radius: 3px; padding: 2px 4                                  text-color); display: flex; align-items: center; justify-content: center; height: 20px; }
              ist-btn:active {               ist-btn:active {               ist-btn:activ.chat              ist-btn:active {               ist-btn:active {               ist-btn:activ.chat              ist-btn:active {               ist-btn:active {               ist-btn:activ.chat              ist-btnnt = pattern.sub(replacement, content)

with open('tools/httpserver/templates/with open('tools/httpserver/templates/with open('tools/httpsertent)
