import assert from 'node:assert/strict';
import { test } from 'node:test';
import postcss from 'postcss';
import nested from 'postcss-nested';
import tailwindcss from 'tailwindcss';
import autoprefixer from 'autoprefixer';
import config from '../tailwind.config.js';

test('the selector-parser override preserves nested selectors and apply', async () => {
  const result = await postcss([
    nested(),
    tailwindcss({ ...config, content: [{ raw: '<div class="card"></div>' }] }),
    autoprefixer(),
  ]).process(`.card {
    &:hover, &:focus-visible { @apply flex; }
    & > [data-state="ready"] { @apply opacity-100; }
  }`, { from: undefined });
  const rules = new Map();
  result.root.walkRules(rule => rules.set(rule.selector, rule));
  assert.equal(rules.get('.card:hover, .card:focus-visible')?.nodes[0].prop, 'display');
  assert.equal(rules.get('.card:hover, .card:focus-visible')?.nodes[0].value, 'flex');
  assert.equal(rules.get('.card > [data-state="ready"]')?.nodes[0].value, '1');
  assert.ok(!result.css.includes('@apply'));
});

test('Tailwind keeps responsive, group, hover, and arbitrary child variants', async () => {
  const result = await postcss([
    tailwindcss({ ...config, content: [{ raw: '<button class="hover:bg-brand-500 md:flex group-hover:opacity-100 [&>svg]:h-4"></button>' }] }),
    autoprefixer(),
  ]).process('@tailwind utilities;', { from: undefined });
  const rules = new Map();
  result.root.walkRules(rule => rules.set(rule.selector, rule));
  assert.ok(rules.has('.hover\\:bg-brand-500:hover'));
  assert.ok(rules.has('.group:hover .group-hover\\:opacity-100'));
  assert.ok([...rules.keys()].some(selector => selector.includes('svg') && selector.endsWith('>svg')));
  const responsive = rules.get('.md\\:flex');
  assert.equal(responsive?.parent.type, 'atrule');
  assert.equal(responsive?.parent.name, 'media');
  assert.equal(responsive?.parent.params, '(min-width: 768px)');
});
