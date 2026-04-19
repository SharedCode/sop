import os

filepath = "templates/scripts_part01.html"
with open(filepath, "r") as f:
    text = f.read()

orig_stores = """                // Remove filter that prevents vectorDBs to be displayed
                stores.forEach(store => {
                    const li = document.createElement('li');
                    li.textContent = store.charAt(0).toUpperCase() + store.slice(1);
                    li.onclick = () => selectStore(store, li);
                    list.appendChild(li);

                    if (storeToSelect && store === storeToSelect) {
                        selectStore(store, li);
                    }
                });"""

new_stores = """                stores.forEach(store => {
                    const li = document.createElement('li');
                    li.textContent = store.charAt(0).toUpperCase() + store.slice(1);
                    li.onclick = () => selectStore(store, li);
                    list.appendChild(li);

                    if (storeToSelect && store === storeToSelect) {
                        selectStore(store, li);
                    }
                });"""

text = text.replace(orig_stores, new_stores)

with open(filepath, "w") as f:
    f.write(text)

print("done script fix for part 1")
with open("templates/scripts_part01.html", "r") as f:
    text = f.read()

# Replace the stores iteration
orig_stores = """                // Remove filter that prevents vectorDBs to be displayed
                stores.forEach(store => {
                    const li = document.createElement('li');
                    li.textContent = store.charAt(0).toUpperCase() + store.slice(1);
                    li.onclick = () => selectStore(store, li);
                    list.appendChild(li);

                    if (storeToSelect && store === storeToSelect) {
                        selectStore(store, li);
                    }
                });"""

new_stores = """                stores.forEach(store => {
                    const li = document.createElement('li');
                    li.textContent = store.charAt(0).toUpperCase() + store.slice(1);
                    li.onclick = () => selectStore(store, li);
                    list.appendChild(li);

                    if (st                    if (st        le                    if (st                    if (i);
                    }                    }""

text =text =text =text =text =text =text =text =text =text =text =text =text =text =text "w") astext =text =text =text =text =text =tript fix for part 1")
