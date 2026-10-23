// Command palette (Ctrl+K): ค้นหาทีม / นักเตะ / ลีก / เมนู แบบใกล้เคียง (ไทย-อังกฤษ)
(function () {
  const box = document.getElementById('cmdk'), input = document.getElementById('cmdk-q'), list = document.getElementById('cmdk-list');
  const PAGES = [['/', 'หน้าแรก', '🏠'], ['/standings', 'ตารางคะแนน', '📊'], ['/scorers', 'ดาวซัลโว', '⚽'],
                 ['/matches', 'ผลบอล', '📅'], ['/pipeline', 'สถานะระบบ', '🛠']];
  let index = null, items = [], active = 0;

  const norm = s => (s || '').toLowerCase().replace(/\s+/g, '');
  function score(text, q) {  // 3 = ขึ้นต้น, 2 = มีคำนี้, 1 = ตัวอักษรเรียงตามลำดับ (พิมพ์ไม่ครบ/ข้ามตัว)
    const t = norm(text); if (!q) return 0;
    if (t.startsWith(q)) return 3;
    if (t.includes(q)) return 2;
    let j = 0; for (const ch of t) { if (ch === q[j]) j++; if (j === q.length) return 1; }
    return 0;
  }
  const esc = s => String(s ?? '').replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
  function hl(text, raw) {
    const t = String(text || ''), i = raw ? t.toLowerCase().indexOf(raw.toLowerCase()) : -1;
    return i < 0 ? esc(t) : esc(t.slice(0, i)) + '<mark>' + esc(t.slice(i, i + raw.length)) + '</mark>' + esc(t.slice(i + raw.length));
  }
  const seasonArg = () => window.CMDK_SEASON ? '&season=' + encodeURIComponent(window.CMDK_SEASON) : '';

  async function load() {
    if (index) return;
    try {
      const qs = window.CMDK_SEASON ? '?season=' + encodeURIComponent(window.CMDK_SEASON) : '';
      index = await (await fetch('/api/search-index' + qs)).json();
    } catch (e) { index = { teams: [], players: [], leagues: [] }; }
  }

  function top(arr, f, n) {
    return arr.map(x => [x, f(x)]).filter(x => x[1] > 0).sort((a, b) => b[1] - a[1]).slice(0, n).map(x => x[0]);
  }

  function render() {
    const raw = input.value.trim(), q = norm(raw);
    const groups = [];
    if (q && index) {
      const teams = top(index.teams, t => Math.max(score(t.th, q), score(t.en, q)), 6);
      const players = top(index.players, p => Math.max(score(p.name, q), score(p.team, q) - 1), 5);
      const leagues = top(index.leagues, l => Math.max(score(l.th, q), score(l.en, q), score(l.code, q)), 4);
      if (teams.length) groups.push(['ทีม', teams.map(t => ({
        icon: '🛡', html: hl(t.th, raw) + (t.en && t.en !== t.th ? ' <small>' + hl(t.en, raw) + '</small>' : ''),
        meta: t.lgName + ' · อันดับ ' + t.pos + ' · ' + t.pts + ' แต้ม',
        href: '/standings?league=' + t.lg + '&team=' + encodeURIComponent(t.th) + seasonArg() }))]);
      if (players.length) groups.push(['นักเตะ', players.map(p => ({
        icon: '👤', html: hl(p.name, raw) + ' <small>' + hl(p.team, raw) + '</small>', meta: p.goals + ' ประตู · ' + p.lgName,
        href: '/scorers?league=' + p.lg + '&team=' + encodeURIComponent(p.name) + seasonArg() }))]);
      if (leagues.length) groups.push(['ลีก', leagues.map(l => ({
        icon: '🏆', html: hl(l.th, raw) + ' <small>' + hl(l.en, raw) + '</small>', meta: 'ตารางคะแนน',
        href: '/standings?league=' + l.code + seasonArg() }))]);
      if (document.querySelector('form.filterbar')) groups.push(['ตัวกรอง', [{
        icon: '⌕', html: 'กรองหน้านี้ด้วยคำว่า "<b>' + esc(raw) + '</b>"', meta: '', filter: raw }]]);
    }
    const pages = PAGES.filter(p => !q || score(p[1], q) || score(p[0], q));
    if (pages.length) groups.push(['เมนู', pages.map(p => ({ icon: p[2], html: 'ไปหน้า ' + hl(p[1], raw), meta: '', href: p[0] }))]);

    items = []; let html = '';
    for (const [title, arr] of groups) {
      html += '<div class="cmdk-group">' + title + '</div>';
      for (const it of arr) {
        html += '<div class="cmdk-item" role="option" data-i="' + items.length + '"><span class="ic">' + it.icon +
                '</span><span class="tx">' + it.html + '</span><span class="mt">' + esc(it.meta) + '</span></div>';
        items.push(it);
      }
    }
    list.innerHTML = html || '<div class="cmdk-empty">ไม่พบ "' + esc(raw) + '" ลองสะกดใหม่หรือพิมพ์ภาษาอังกฤษ</div>';
    active = 0; mark();
  }
  function mark() {
    list.querySelectorAll('.cmdk-item').forEach((el, i) => el.classList.toggle('on', i === active));
    const el = list.querySelector('.cmdk-item.on'); if (el) el.scrollIntoView({ block: 'nearest' });
  }
  function go(it) {
    if (!it) return;
    if (it.filter !== undefined) {  // ตัวกรองเดิม: ส่ง ?team= ของหน้านี้
      const form = document.querySelector('form.filterbar');
      form.querySelector('input[name=team]').value = it.filter; form.submit(); return;
    }
    location.href = it.href;
  }
  async function open() { box.hidden = false; input.value = ''; input.focus(); await load(); render(); }
  function close() { box.hidden = true; }

  document.addEventListener('keydown', e => {
    const typing = /INPUT|TEXTAREA|SELECT/.test(document.activeElement.tagName);
    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'k') { e.preventDefault(); box.hidden ? open() : close(); }
    else if (e.key === '/' && box.hidden && !typing) { e.preventDefault(); open(); }
    else if (!box.hidden) {
      if (e.key === 'Escape') close();
      else if (e.key === 'ArrowDown') { e.preventDefault(); active = Math.min(active + 1, items.length - 1); mark(); }
      else if (e.key === 'ArrowUp') { e.preventDefault(); active = Math.max(active - 1, 0); mark(); }
      else if (e.key === 'Enter') { e.preventDefault(); go(items[active]); }
    }
  });
  input.addEventListener('input', render);
  list.addEventListener('mousemove', e => {
    const el = e.target.closest('.cmdk-item');
    if (el && +el.dataset.i !== active) { active = +el.dataset.i; mark(); }
  });
  list.addEventListener('click', e => { const el = e.target.closest('.cmdk-item'); if (el) go(items[+el.dataset.i]); });
  box.addEventListener('mousedown', e => { if (e.target === box) close(); });
  document.querySelectorAll('[data-cmdk-open]').forEach(b => b.addEventListener('click', open));
})();
