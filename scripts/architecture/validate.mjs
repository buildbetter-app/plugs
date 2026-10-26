#!/usr/bin/env node
/** Validate Markdown source links, heading anchors, and every Mermaid diagram.
 * Usage: node scripts/architecture/validate.mjs [--links-only]
 * Uses the same isolated pinned tools and browser settings as render.mjs.
 */
import { promises as fs } from 'node:fs';
import path from 'node:path';
import { root, tooling, documents, resolveLink, mermaidBrowser, slug } from './render.mjs';

const tools = await tooling();
const docs = await documents(tools.Marked);
const errors = []; let linkCount = 0; let diagramCount = 0;
const markdown = new tools.Marked({ gfm: true });
const anchorCache = new Map(docs.map(doc => [doc.filename, new Set(doc.headings.map(heading => heading.id))]));
async function anchors(filename) {
  if (anchorCache.has(filename)) return anchorCache.get(filename);
  const source = await fs.readFile(filename, 'utf8'); const result = new Set(); const seen = new Map();
  markdown.walkTokens(markdown.lexer(source), token => {
    if (token.type !== 'heading') return;
    const plain = token.text.replace(/\[([^\]]+)\]\([^)]*\)/g, '$1').replace(/[*`_]/g, '');
    const base = slug(plain); const count = seen.get(base) || 0; seen.set(base, count + 1);
    result.add(count ? `${base}-${count}` : base);
  });
  for (const match of source.matchAll(/\bid=["']([^"']+)["']/g)) result.add(match[1]);
  anchorCache.set(filename, result); return result;
}
for (const doc of docs) {
  for (const href of doc.links) {
    let resolved;
    try { resolved = resolveLink(href, doc.filename); }
    catch (error) { errors.push(`${doc.name}: malformed link ${href}: ${error.message}`); continue; }
    if (!resolved) continue; linkCount++;
    if (resolved.target !== root && !resolved.target.startsWith(`${root}${path.sep}`)) { errors.push(`${doc.name}: link escapes repository: ${href}`); continue; }
    let stat;
    try { stat = await fs.stat(resolved.target); }
    catch { errors.push(`${doc.name}: missing link target: ${href}`); continue; }
    if (stat.isFile() && /\.md$/i.test(resolved.target) && resolved.fragment && !/^L\d+(?:-L?\d+)?$/.test(resolved.fragment)) {
      if (!(await anchors(resolved.target)).has(resolved.fragment)) errors.push(`${doc.name}: missing heading anchor: ${href}`);
    }
    if (stat.isFile() && /^L\d+(?:-L?\d+)?$/.test(resolved.fragment)) {
      const line = Number(resolved.fragment.match(/^L(\d+)/)[1]);
      const lines = (await fs.readFile(resolved.target, 'utf8')).split('\n').length;
      if (line < 1 || line > lines) errors.push(`${doc.name}: source line ${line} exceeds ${lines} lines: ${href}`);
    }
  }
}
if (!process.argv.includes('--links-only')) {
  const { browser, page } = await mermaidBrowser(tools);
  try {
    for (const doc of docs) {
      for (const [index, definition] of doc.diagrams.entries()) {
        diagramCount++;
        try { await page.evaluate(async definition => { await window.mermaid.parse(definition); }, definition); }
        catch (error) { errors.push(`${doc.name}: Mermaid diagram ${index + 1}: ${error.message}`); }
      }
    }
  } finally { await browser.close(); }
}
if (!docs.length) errors.push('No architecture guide Markdown files found.');
if (errors.length) { console.error(errors.join('\n\n')); console.error(`\nFAILED: ${errors.length} errors across ${docs.length} documents.`); process.exitCode = 1; }
else console.log(`PASS: ${docs.length} documents, ${linkCount} local links, ${diagramCount} Mermaid diagrams${process.argv.includes('--links-only') ? ' (diagram validation skipped)' : ''}.`);
