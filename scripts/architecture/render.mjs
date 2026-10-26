#!/usr/bin/env node
/**
 * Build an entirely offline architecture guide, including Mermaid SVG diagrams.
 * Usage: node scripts/architecture/render.mjs [--output /absolute/path]
 * Requirements: Node >=20, npm, and a Chromium browser (Chrome/Brave are discovered).
 * First run installs pinned rendering tools into the user cache, never this repo.
 * Set ARCHITECTURE_TOOLS_DIR to a pre-provisioned tools directory for offline builds.
 * Set CHROME_PATH to a Chromium executable when automatic discovery is unsuitable.
 */
import { promises as fs, existsSync, readFileSync, statSync } from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { createRequire } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';

export const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
export const guide = path.join(root, 'engdocs/architecture-guide');
const packages = ['marked@17.0.5', 'mermaid@11.12.0', 'playwright@1.56.1', 'vis-network@9.1.6'];
const escapeHTML = text => String(text).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;');
export const slug = text => text.toLowerCase().replace(/<[^>]+>/g, '').replace(/[^\p{L}\p{N}_\-\s]/gu, '').trim().replace(/\s/g, '-');
const key = text => createHash('sha256').update(text).digest('hex').slice(0, 12);

export async function tooling() {
  const directory = process.env.ARCHITECTURE_TOOLS_DIR || path.join(os.homedir(), '.cache', 'beads-architecture-tools', 'v1');
  const require = createRequire(path.join(directory, 'package.json'));
  const present = packages.every(specifier => {
    const [name, version] = specifier.split("@");
    try { return JSON.parse(readFileSync(path.join(directory, "node_modules", name, "package.json"), "utf8")).version === version; } catch { return false; }
  });
  if (!present) {
    if (process.env.ARCHITECTURE_TOOLS_DIR) throw new Error(`ARCHITECTURE_TOOLS_DIR lacks the pinned tools: ${packages.join(', ')}`);
    await fs.mkdir(directory, { recursive: true });
    console.log(`Installing isolated rendering tools in ${directory}`);
    execFileSync(process.platform === 'win32' ? 'npm.cmd' : 'npm', ['install', '--prefix', directory, '--no-save', '--no-package-lock', '--ignore-scripts', ...packages], { stdio: 'inherit' });
  }
  const { Marked } = await import(pathToFileURL(require.resolve('marked')).href);
  const { chromium } = require('playwright');
  return { Marked, chromium, mermaidPath: path.join(directory, 'node_modules/mermaid/dist/mermaid.min.js'), visNetworkPath: path.join(directory, 'node_modules/vis-network/standalone/umd/vis-network.min.js') };
}

export async function mermaidBrowser(tools) {
  const candidates = [
    process.env.CHROME_PATH,
    '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    '/Applications/Brave Browser.app/Contents/MacOS/Brave Browser',
    '/usr/bin/chromium', '/usr/bin/chromium-browser', '/usr/bin/google-chrome',
    '/opt/google/chrome/chrome',
  ].filter(Boolean);
  const executablePath = candidates.find(existsSync);
  let browser;
  try { browser = await tools.chromium.launch({ headless: true, ...(executablePath ? { executablePath } : {}) }); }
  catch (error) { throw new Error(`A Chromium browser is required. Set CHROME_PATH or install one with: npx playwright@1.56.1 install chromium\n${error.message}`); }
  const page = await browser.newPage({ viewport: { width: 1600, height: 1200 }, deviceScaleFactor: 1 });
  await page.route('**/*', route => route.abort());
  await page.setContent('<!doctype html><html><head><style>body{font-family:Arial,sans-serif}</style></head><body></body></html>');
  await page.addScriptTag({ path: tools.mermaidPath });
  await page.evaluate(() => window.mermaid.initialize({ startOnLoad: false, securityLevel: 'strict', theme: 'base', suppressErrorRendering: true, fontFamily: 'Arial, sans-serif', themeVariables: { primaryColor: '#edf4ff', primaryBorderColor: '#5174a5', primaryTextColor: '#17324f', secondaryColor: '#e5f6f1', tertiaryColor: '#fff2d6', lineColor: '#62788f', fontSize: '16px' }, flowchart: { htmlLabels: false, useMaxWidth: true, curve: 'basis' }, sequence: { useMaxWidth: true } }));
  return { browser, page };
}

export async function documents(Marked) {
  const markdown = new Marked({ gfm: true });
  const names = (await fs.readdir(guide)).filter(name => name.endsWith('.md')).sort((a, b) => a === 'README.md' ? -1 : b === 'README.md' ? 1 : a.localeCompare(b));
  return Promise.all(names.map(async name => {
    const filename = path.join(guide, name);
    const source = await fs.readFile(filename, 'utf8');
    const tokens = markdown.lexer(source);
    const headings = []; const links = []; const images = []; const diagrams = []; const slugs = new Map();
    markdown.walkTokens(tokens, token => {
      if (token.type === 'heading') {
        const plain = token.text.replace(/\[([^\]]+)\]\([^)]*\)/g, '$1').replace(/[*`_]/g, '');
        const base = slug(plain); const seen = slugs.get(base) || 0; slugs.set(base, seen + 1);
        headings.push({ depth: token.depth, title: plain, id: seen ? `${base}-${seen}` : base });
      }
      if (token.type === 'link' || token.type === 'image') links.push(token.href);
      if (token.type === 'image') images.push(token.href);
      if (token.type === 'code' && /^mermaid(?:\s|$)/.test(token.lang || '')) diagrams.push(token.text);
    });
    return { filename, name, source, tokens, headings, links, images, diagrams, title: headings.find(h => h.depth === 1)?.title || name.replace(/\.md$/, ''), output: name === 'README.md' ? 'index.html' : name.replace(/\.md$/, '.html') };
  }));
}

export function resolveLink(href, filename) {
  if (/^(?:[a-z][a-z0-9+.-]*:|\/\/)/i.test(href)) return null;
  const [beforeHash, fragment = ''] = href.split('#', 2);
  const decoded = decodeURIComponent(beforeHash.split('?')[0]);
  const line = decoded.match(/:(\d+)(?:-\d+)?$/)?.[1];
  const clean = line ? decoded.replace(/:\d+(?:-\d+)?$/, '') : decoded;
  const target = clean ? path.resolve(clean.startsWith('/') ? root : path.dirname(filename), clean.replace(/^\//, '')) : filename;
  return { target, fragment: line ? `L${line}` : decodeURIComponent(fragment), originalFragment: fragment };
}

async function render(output) {
  const tools = await tooling();
  const docs = await documents(tools.Marked);
  if (!docs.length) throw new Error(`No Markdown pages found in ${guide}`);
  const { browser, page } = await mermaidBrowser(tools);
  const byPath = new Map(docs.map(doc => [doc.filename, doc]));
  const sources = new Map(); const images = new Map(); const artifacts = new Map();
  await fs.mkdir(path.join(output, 'assets/diagrams'), { recursive: true });
  const assetSource = path.join(guide, 'assets');
  await fs.cp(assetSource, path.join(output, 'assets'), { recursive: true });
  for (const doc of docs) for (const href of doc.images) {
    const resolved = resolveLink(href, doc.filename);
    if (resolved && resolved.target.startsWith(`${root}${path.sep}`) && existsSync(resolved.target) && statSync(resolved.target).isFile()) {
      const destination = `assets/media/${key(resolved.target)}-${path.basename(resolved.target)}`;
      await fs.mkdir(path.join(output, 'assets/media'), { recursive: true });
      await fs.copyFile(resolved.target, path.join(output, destination)); images.set(resolved.target, destination);
    }
  }
  const search = docs.map(doc => ({ title: doc.title, url: doc.output, text: doc.source.replace(/```[\s\S]*?```/g, '').replace(/[#*`_|>]/g, '').replace(/\[([^\]]+)\]\([^)]*\)/g, '$1').replace(/\s+/g, ' ').trim() }));
  await fs.writeFile(path.join(output, 'assets/search-index.js'), `window.architectureSearch=${JSON.stringify(search).replaceAll('<', '\\u003c')};\n`);
  let diagramCount = 0; const usedDiagrams = new Set();
  try {
    for (const doc of docs) {
      let diagramIndex = 0; let headingIndex = 0;
      const svgNames = [];
      for (const definition of doc.diagrams) {
        const basename = `${path.basename(doc.output, '.html')}-${++diagramIndex}-${key(definition)}.svg`;
        const svg = await page.evaluate(async ({ definition, id }) => {
          await window.mermaid.parse(definition);
          const { svg } = await window.mermaid.render(id, definition);
          const container = document.createElement('div'); container.innerHTML = svg;
          const element = container.querySelector('svg');
          if (!element) throw new Error('Mermaid returned no SVG');
          // Mermaid's base ER theme applies one fill to both the label box and
          // its text. Give each an explicit color so relationship names remain
          // legible in the standalone SVG as well as inside the viewer.
          if (/^\s*erDiagram\b/.test(definition)) {
            element.querySelectorAll('.edgeLabel text, .edgeLabel tspan').forEach(label => { label.style.fill = '#17324f'; });
            element.querySelectorAll('.edgeLabel rect').forEach(box => { box.style.fill = '#fff2d6'; });
          }
          const serialized = new XMLSerializer().serializeToString(element);
          if (new DOMParser().parseFromString(serialized, 'image/svg+xml').querySelector('parsererror')) throw new Error('Generated SVG is not valid XML');
          return serialized;
        }, { definition, id: `d${key(basename)}` });
        await fs.writeFile(path.join(output, 'assets/diagrams', basename), svg);
        svgNames.push(basename); usedDiagrams.add(basename); diagramCount++;
      }
      diagramIndex = 0;
      const markdown = new tools.Marked({ gfm: true, renderer: {
        heading(token) { const heading = doc.headings[headingIndex++]; return `<h${token.depth} id="${escapeHTML(heading.id)}"><a class="anchor" href="#${escapeHTML(heading.id)}" aria-label="Link to this section">#</a>${this.parser.parseInline(token.tokens)}</h${token.depth}>\n`; },
        code(token) {
          if (/^mermaid(?:\s|$)/.test(token.lang || '')) {
            const src = `assets/diagrams/${svgNames[diagramIndex++]}`;
            return `<figure class="diagram"><a href="${src}" target="_blank" rel="noopener" title="Open full-size diagram"><img src="${src}" alt="${escapeHTML(`Diagram ${diagramIndex}: ${doc.title}`)}" loading="lazy"></a><figcaption>Diagram ${diagramIndex} <button class="diagram-expand" data-src="${src}" type="button">Expand diagram ↗</button><details><summary>Mermaid source</summary><pre><code>${escapeHTML(token.text)}</code></pre></details></figcaption></figure>\n`;
          }
          return `<div class="code-block"><span class="code-language">${escapeHTML(token.lang || 'text')}</span><pre><code>${escapeHTML(token.text)}</code></pre></div>\n`;
        },
        image(token) {
          const resolved = resolveLink(token.href, doc.filename); const src = resolved ? images.get(resolved.target) || token.href : token.href;
          return `<img src="${escapeHTML(src)}" alt="${escapeHTML(token.text)}"${token.title ? ` title="${escapeHTML(token.title)}"` : ''} loading="lazy">`;
        },
        link(token) {
          const resolved = resolveLink(token.href, doc.filename); let href = token.href;
          if (resolved) {
            const linkedDoc = byPath.get(resolved.target);
            const generatedPath = path.relative(path.join(guide, 'site'), resolved.target);
            if (generatedPath && !generatedPath.startsWith('..') && !path.isAbsolute(generatedPath)) href = generatedPath.replaceAll(path.sep, '/') + (resolved.fragment ? `#${resolved.fragment}` : '');
            else if (resolved.target.startsWith(`${guide}${path.sep}`) && /\.html$/i.test(resolved.target) && existsSync(resolved.target)) {
              const destination = `artifacts/${path.relative(guide, resolved.target)}`; artifacts.set(resolved.target, destination); href = destination + (resolved.fragment ? `#${resolved.fragment}` : '');
            }
            else if (linkedDoc) href = linkedDoc.output + (resolved.fragment ? `#${resolved.fragment}` : '');
            else if (resolved.target.startsWith(`${root}${path.sep}`) && existsSync(resolved.target)) {
              const relative = path.relative(root, resolved.target); const destination = statSync(resolved.target).isDirectory() ? `source/${relative}/index.html` : `source/${relative}.html`;
              sources.set(resolved.target, destination);
              href = destination + (resolved.fragment ? `#${resolved.fragment}` : '');
            }
          }
          return `<a href="${escapeHTML(href)}"${token.title ? ` title="${escapeHTML(token.title)}"` : ''}>${this.parser.parseInline(token.tokens)}</a>`;
        },
      } });
      const content = markdown.parse(doc.source);
      await fs.writeFile(path.join(output, doc.output), shell(doc, docs, content));
      console.log(`${doc.output}: ${doc.diagrams.length} diagrams`);
    }
  } finally { await browser.close(); }
  for (const basename of await fs.readdir(path.join(output, 'assets/diagrams'))) {
    if (/-\d+-[a-f0-9]{12}\.svg$/.test(basename) && !usedDiagrams.has(basename)) await fs.unlink(path.join(output, 'assets/diagrams', basename));
  }
  for (const [filename, destination] of artifacts) {
    let html = await fs.readFile(filename, 'utf8');
    const vendorReference = path.relative(path.dirname(destination), 'assets/vendor/vis-network.min.js').replaceAll(path.sep, '/');
    if (html.includes('https://unpkg.com/vis-network@9.1.6/standalone/umd/vis-network.min.js')) {
      await fs.mkdir(path.join(output, 'assets/vendor'), { recursive: true });
      await fs.copyFile(tools.visNetworkPath, path.join(output, 'assets/vendor/vis-network.min.js'));
      await fs.copyFile(path.resolve(tools.visNetworkPath, '../../../LICENSE-MIT'), path.join(output, 'assets/vendor/vis-network-LICENSE-MIT.txt'));
      html = html.replace(/<script\s+src="https:\/\/unpkg\.com\/vis-network@9\.1\.6\/standalone\/umd\/vis-network\.min\.js"[^>]*>/, `<script src="${vendorReference}">`);
    }
    if (/<(?:script|link|img)[^>]+(?:src|href)=["']https?:/i.test(html)) throw new Error(`Linked HTML artifact still has remote resources: ${filename}`);
    await fs.mkdir(path.dirname(path.join(output, destination)), { recursive: true });
    await fs.writeFile(path.join(output, destination), html);
  }
  for (const [filename, destination] of sources) {
    const stat = await fs.stat(filename);
    const relative = path.relative(root, filename);
    const prefix = '../'.repeat(destination.split('/').length - 1);
    const source = stat.isDirectory() ? `Directory snapshot: ${relative}/\n\n${(await fs.readdir(filename, { withFileTypes: true })).sort((a, b) => a.name.localeCompare(b.name)).map(entry => entry.name + (entry.isDirectory() ? '/' : '')).join('\n')}` : await fs.readFile(filename, 'utf8');
    const headingLines = new Map();
    if (/\.md$/i.test(filename)) {
      let cursor = 0; const seen = new Map(); const markdown = new tools.Marked();
      markdown.walkTokens(markdown.lexer(source), token => {
        if (token.type !== 'heading') return;
        const offset = source.indexOf(token.raw, cursor); cursor = Math.max(cursor, offset + token.raw.length);
        const line = source.slice(0, Math.max(0, offset)).split('\n').length;
        const base = slug(token.text.replace(/\[([^\]]+)\]\([^)]*\)/g, '$1').replace(/[*`_]/g, '')); const count = seen.get(base) || 0; seen.set(base, count + 1);
        headingLines.set(line, count ? `${base}-${count}` : base);
      });
    }
    const lines = source.split('\n').map((line, index) => `<span class="source-line" id="L${index + 1}"><a href="#L${index + 1}" class="line-number">${index + 1}</a>${headingLines.has(index + 1) ? `<span id="${escapeHTML(headingLines.get(index + 1))}"></span>` : ''}${escapeHTML(line) || ' '}</span>`).join('');
    const html = `<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${escapeHTML(relative)} · Beads source</title><link rel="stylesheet" href="${prefix}assets/theme.css"><body class="source-page"><header><a href="${prefix}index.html">← Beads architecture guide</a><h1>${escapeHTML(relative)}</h1><p>Source snapshot from this guide’s checkout. Each line has a stable link.</p></header><pre class="source-code"><code>${lines}</code></pre></body></html>`;
    await fs.mkdir(path.dirname(path.join(output, destination)), { recursive: true });
    await fs.writeFile(path.join(output, destination), html);
  }
  await fs.writeFile(path.join(output, 'build-manifest.json'), JSON.stringify({ generatedAt: new Date().toISOString(), documents: docs.length, diagrams: diagramCount, sourceFiles: sources.size, htmlArtifacts: artifacts.size, tools: packages }, null, 2) + '\n');
  console.log(`Rendered ${docs.length} pages, ${diagramCount} diagrams, ${sources.size} linked source files.\nOpen ${path.join(output, 'index.html')}`);
}

function shell(doc, docs, content) {
  const navigation = docs.map(item => `<a href="${item.output}"${item === doc ? ' aria-current="page"' : ''}>${escapeHTML(item.title)}</a>`).join('');
  const toc = doc.headings.filter(heading => heading.depth > 1 && heading.depth < 4).map(heading => `<a class="depth-${heading.depth}" href="#${escapeHTML(heading.id)}">${escapeHTML(heading.title)}</a>`).join('');
  const index = docs.indexOf(doc);
  return `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="color-scheme" content="light"><title>${escapeHTML(doc.title)} · Beads architecture</title><link rel="stylesheet" href="assets/theme.css"><script src="assets/search-index.js" defer></script><script src="assets/app.js" defer></script></head>
<body><a class="skip-link" href="#main">Skip to content</a><header class="mobile-header"><a href="index.html">BEADS / Architecture</a><button id="nav-toggle" type="button" aria-expanded="false" aria-controls="navigation">Contents</button></header><aside class="sidebar" id="navigation"><a class="brand" href="index.html"><span class="brand-icon">b.</span><span>BEADS<strong>Architecture field guide</strong></span></a><div class="search"><label for="search">Search this guide</label><input id="search" type="search" placeholder="Storage, claim, plugins…" autocomplete="off"><div id="search-results" role="region" aria-live="polite"></div></div><nav aria-label="Chapters">${navigation}</nav><p class="sidebar-note">Offline edition · diagrams include editable Mermaid source.</p></aside><main id="main"><div class="eyebrow">BEADS · SYSTEM ARCHITECTURE</div><article>${content}</article><nav class="pagination" aria-label="Adjacent chapters">${index > 0 ? `<a href="${docs[index - 1].output}">← ${escapeHTML(docs[index - 1].title)}</a>` : '<span></span>'}${index < docs.length - 1 ? `<a href="${docs[index + 1].output}">${escapeHTML(docs[index + 1].title)} →</a>` : ''}</nav><footer>Generated from the Markdown architecture guide. Source links open a local snapshot.</footer></main><aside class="toc"><span>ON THIS PAGE</span><nav aria-label="On this page">${toc}</nav></aside><dialog id="diagram-dialog"><form method="dialog"><button aria-label="Close diagram">Close ×</button></form><div class="diagram-viewport"><img alt="Expanded architecture diagram"></div><a class="diagram-original" target="_blank" rel="noopener">Open original SVG ↗</a></dialog></body></html>`;
}

const entrypoint = process.argv[1] ? await fs.realpath(process.argv[1]).catch(() => null) : null;
if (entrypoint === fileURLToPath(import.meta.url)) {
  const args = process.argv.slice(2);
  if (args.includes('--help')) { console.log('Usage: node scripts/architecture/render.mjs [--output PATH]\nFirst run downloads isolated pinned tools. CHROME_PATH and ARCHITECTURE_TOOLS_DIR override browser and tools.'); process.exit(0); }
  const option = args.indexOf('--output');
  if (option !== -1 && !args[option + 1]) throw new Error('--output requires a directory');
  const output = path.resolve(option === -1 ? path.join(guide, 'site') : args[option + 1]);
  await render(output).catch(error => { console.error(error.stack || error); process.exitCode = 1; });
}
