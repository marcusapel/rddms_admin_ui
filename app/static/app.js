(function () {
  function el(tag, attrs, text) {
    const e = document.createElement(tag);
    if (attrs) for (const [k, v] of Object.entries(attrs)) {
      if (k === 'class') e.className = v;
      else if (k === 'style' && typeof v === 'object') Object.assign(e.style, v);
      else e.setAttribute(k, v);
    }
    if (text) e.textContent = text;
    return e;
  }

  function linkSpan(href, text, cls = 'navlink') {
    const s = el('span', { 'data-href': href, 'class': cls }, text);
    s.addEventListener('click', () => { window.location.assign(href); });
    return s;
  }

  function buildHeader() {
    const root = document.getElementById('nav-root');
    if (!root) return;

    const current = root.getAttribute('data-current-path') || '/';

    // Clear placeholder text
    root.textContent = '';

    // HOME
    root.appendChild(linkSpan('/', 'Home'));
    // SEARCH
    root.appendChild(linkSpan('/search', 'Search'));

    root.appendChild(linkSpan('/keys', 'Keys')); 

    // separator
    root.appendChild(el('span', { 'class': 'muted' }, ' | '));

    // LOGIN (auto) — preserve ?next=current
    //const encNext = encodeURIComponent(current);
    //root.appendChild(linkSpan('/login/auto?next=' + encNext, 'Login'));
    // LOGOUT
    //root.appendChild(linkSpan('/logout', 'Logout'));
  }

  // Build nav asap after DOM is parsed
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', buildHeader);
  } else {
    buildHeader();
  }

  // Global navigation for any element with data-href (safety net)
  document.addEventListener('click', function (e) {
    const el = e.target.closest('[data-href]');
    if (!el) return;
    const url = el.getAttribute('data-href');
    if (url) window.location.assign(url);
  });
})();