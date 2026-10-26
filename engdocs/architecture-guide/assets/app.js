/* Offline navigation, chapter search, and diagram expansion. No network calls. */
(() => {
  const search = document.querySelector('#search');
  const results = document.querySelector('#search-results');
  const index = window.architectureSearch || [];
  search?.addEventListener('input', () => {
    results.replaceChildren();
    const query = search.value.trim().toLowerCase();
    if (query.length < 2) return;
    const terms = query.split(/\s+/);
    const matches = index.map(item => ({ ...item, score: terms.reduce((total, term) => total + (item.title.toLowerCase().includes(term) ? 5 : 0) + (item.text.toLowerCase().includes(term) ? 1 : 0), 0) })).filter(item => terms.every(term => (item.title + ' ' + item.text).toLowerCase().includes(term))).sort((a, b) => b.score - a.score).slice(0, 10);
    for (const item of matches) {
      const link = document.createElement('a'); link.href = item.url;
      const title = document.createElement('strong'); title.textContent = item.title; link.append(title);
      const at = Math.max(0, item.text.toLowerCase().indexOf(terms[0]) - 45);
      const snippet = document.createElement('p'); snippet.textContent = (at ? '…' : '') + item.text.slice(at, at + 155) + '…'; link.append(snippet); results.append(link);
    }
    if (!matches.length) { const empty = document.createElement('p'); empty.className = 'no-results'; empty.textContent = 'No matching chapters.'; results.append(empty); }
  });
  document.querySelector('#nav-toggle')?.addEventListener('click', event => { const open = document.querySelector('#navigation').classList.toggle('open'); event.currentTarget.setAttribute('aria-expanded', String(open)); });
  document.addEventListener('keydown', event => { if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') { event.preventDefault(); document.querySelector('#navigation')?.classList.add('open'); search?.focus(); } });
  const dialog = document.querySelector('#diagram-dialog');
  document.querySelectorAll('.diagram-expand').forEach(button => button.addEventListener('click', () => { dialog.querySelector('img').src = button.dataset.src; dialog.querySelector('a').href = button.dataset.src; dialog.showModal(); }));
  dialog?.addEventListener('click', event => { if (event.target === dialog) dialog.close(); });
})();
