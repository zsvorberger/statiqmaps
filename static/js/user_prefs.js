(function(){
  const PREF_KEY = 'sst:prefs';
  const DEFAULTS = {
    theme: 'system',
    density: 'cozy',
    defaultMap: 'terrain',
    emphasizeNew: true,
    autoSync: false,
  };

  let prefs = loadPrefs();
  const listeners = new Set();
  const media = window.matchMedia ? window.matchMedia('(prefers-color-scheme: dark)') : null;

  function loadPrefs(){
    try{
      const raw = localStorage.getItem(PREF_KEY);
      if(!raw) return { ...DEFAULTS };
      const parsed = JSON.parse(raw);
      return { ...DEFAULTS, ...parsed };
    }catch(e){
      console.warn('Prefs load failed', e);
      return { ...DEFAULTS };
    }
  }

  function persist(){
    try{
      localStorage.setItem(PREF_KEY, JSON.stringify(prefs));
    }catch(e){
      console.warn('Prefs save failed', e);
    }
  }

  function resolvedTheme(){
    if(prefs.theme === 'light' || prefs.theme === 'dark') return prefs.theme;
    return media && media.matches ? 'dark' : 'light';
  }

  function applyToDom(){
    const doc = document.documentElement;
    doc.dataset.prefTheme = prefs.theme;
    doc.dataset.theme = resolvedTheme();
    doc.dataset.density = prefs.density;
    doc.dataset.defaultMap = prefs.defaultMap;
    doc.dataset.emphasizeNew = String(!!prefs.emphasizeNew);
    doc.dataset.autoSync = String(!!prefs.autoSync);
    if(document.body){
      document.body.classList.toggle('pref-compact', prefs.density === 'compact');
    }
  }

  function notify(){
    const payload = { ...prefs };
    listeners.forEach((fn)=>{ try{ fn(payload); }catch(e){ console.warn('Prefs listener failed', e); } });
    window.dispatchEvent(new CustomEvent('sst:prefs-changed', { detail: payload }));
  }

  function setPrefs(next){
    prefs = { ...prefs, ...next };
    persist();
    applyToDom();
    notify();
  }

  function resetPrefs(){
    prefs = { ...DEFAULTS };
    persist();
    applyToDom();
    notify();
  }

  if(media && media.addEventListener){
    media.addEventListener('change', ()=>{
      if(prefs.theme === 'system'){
        applyToDom();
        notify();
      }
    });
  }

  if(document.readyState === 'loading'){
    document.addEventListener('DOMContentLoaded', applyToDom, { once: true });
  }else{
    applyToDom();
  }

  window.UserPrefs = {
    get(){ return { ...prefs }; },
    set(updates){ setPrefs(updates); },
    reset(){ resetPrefs(); },
    defaults: { ...DEFAULTS },
    subscribe(fn){
      if(typeof fn !== 'function') return () => {};
      listeners.add(fn);
      return ()=>listeners.delete(fn);
    },
  };
})();
