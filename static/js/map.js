(function () {
  const setStatus = (m)=>{ const s=document.getElementById('status'); if(s) s.textContent=m; };
  const toast = (m)=>{ const t=document.getElementById('toast'); if(!t) return; t.textContent=m; t.style.display='block'; setTimeout(()=>t.style.display='none',2400); };

  const blankToggle = document.getElementById('blankToggle');
  if (blankToggle) {
    blankToggle.addEventListener('click', ()=>{
      document.body.classList.toggle('ui-blank');
    });
  }

  function pmtilesAsset(name){
    const assetMap = window.PMTILES_ASSETS || {};
    const explicit = (assetMap[name] || '').trim();
    if(explicit){
      const cleanExplicit = explicit.replace(/\/+$/, '');
      return `pmtiles://${cleanExplicit}`;
    }
    const base = (window.PMTILES_BASE_URL || '').trim();
    if(base){
      const cleanBase = base.replace(/\/+$/, '');
      return `pmtiles://${cleanBase}/${name}`;
    }
    return null;
  }
  function initPanelCollapse(panelId, storageKey){
    const panel = document.getElementById(panelId);
    if (!panel) return;
    const toggle = panel.querySelector('.panel-toggle');
    const body = panel.querySelector('.panel-body');
    if (!toggle || !body) return;
    const key = `map:${storageKey || panelId}:collapsed`;
    function setState(collapsed){
      panel.classList.toggle('collapsed', collapsed);
      body.hidden = collapsed;
      toggle.textContent = collapsed ? 'Show' : 'Hide';
      toggle.setAttribute('aria-expanded', String(!collapsed));
    }
    let stored = null;
    try{ stored = localStorage.getItem(key); }catch(_){}
    setState(stored === '1');
    toggle.addEventListener('click', ()=>{
      const next = !panel.classList.contains('collapsed');
      setState(next);
      try{ localStorage.setItem(key, next ? '1' : '0'); }catch(_){}
    });
  }

  setStatus('booting…');
  initPanelCollapse('surfacePanel', 'surface');
  initPanelCollapse('crashPanel', 'crash');
  initPanelCollapse('drawPanel', 'draw');
  initPanelCollapse('usgsPanel', 'usgs');

  const crashYears = [2024, 2023, 2022, 2021, 2020];
  const crashColorByYear = {
    2024: '#e11d48',
    2023: '#f97316',
    2022: '#f59e0b',
    2021: '#22c55e',
    2020: '#06b6d4'
  };
  const crashRowsHtml = crashYears.map((year)=>`
        <div class="row">
          <span><span class="swatch" style="background:${crashColorByYear[year] || '#e11d48'}"></span>Crash locations (${year})</span>
          <input id="chkCrash${year}" type="checkbox" data-layer="crashes" data-year="${year}">
        </div>`).join('');

  function ensureCrashPanel(){
    if (document.getElementById('crashPanel')) return;
    const usgsPanel = document.getElementById('usgsPanel');
    const panelStack = document.getElementById('panelStack');
    const panel = document.createElement('div');
    panel.id = 'crashPanel';
    panel.className = 'collapsible';
    panel.innerHTML = `
      <div class="panel-header">
        <h4>Crashes</h4>
        <button class="panel-toggle" type="button" aria-expanded="true">Hide</button>
      </div>
      <div class="panel-body">
        <div class="row">
          <button id="crashSelectAll" class="btn-small" type="button">Select all</button>
        </div>
        ${crashRowsHtml}
        <div id="crashFilters" class="crash-filters">
          <div class="filter-header">
            <div class="section-title">Filters</div>
            <button id="crashFilterToggle" class="panel-toggle" type="button" aria-expanded="true">Hide</button>
          </div>
          <div id="crashFilterBody" class="filter-body">
            <div class="row">
              <span>Day of week</span>
              <select id="crashFilterDow">
                <option value="">Any</option>
                <option value="1">Sunday</option>
                <option value="2">Monday</option>
                <option value="3">Tuesday</option>
                <option value="4">Wednesday</option>
                <option value="5">Thursday</option>
                <option value="6">Friday</option>
                <option value="7">Saturday</option>
              </select>
            </div>
            <div class="row">
              <span>Month</span>
              <select id="crashFilterMonth">
                <option value="">Any</option>
                <option value="1">January</option>
                <option value="2">February</option>
                <option value="3">March</option>
                <option value="4">April</option>
                <option value="5">May</option>
                <option value="6">June</option>
                <option value="7">July</option>
                <option value="8">August</option>
                <option value="9">September</option>
                <option value="10">October</option>
                <option value="11">November</option>
                <option value="12">December</option>
              </select>
            </div>
            <div class="row">
              <span>Fatal only</span>
              <input id="crashFilterFatal" type="checkbox">
            </div>
            <div class="row">
              <span>Injury only</span>
              <input id="crashFilterInjury" type="checkbox">
            </div>
            <div class="row">
              <span>Bicycle involved</span>
              <input id="crashFilterBicycle" type="checkbox">
            </div>
            <div class="row">
              <span>Pedestrian involved</span>
              <input id="crashFilterPed" type="checkbox">
            </div>
            <div class="row">
              <button id="crashFilterClear" class="btn-small" type="button">Clear filters</button>
            </div>
          </div>
        </div>
        <div id="crashMsg">Crash layer is disabled by default — toggle to load.</div>
      </div>`;
    if (usgsPanel && usgsPanel.parentNode) {
      usgsPanel.parentNode.insertBefore(panel, usgsPanel.nextSibling);
    } else if (panelStack) {
      panelStack.appendChild(panel);
    } else {
      document.body.appendChild(panel);
    }
    initPanelCollapse('crashPanel', 'crash');
  }

  ensureCrashPanel();

  if (!window.maplibregl) { setStatus('MapLibre missing'); return; }

  const map = new maplibregl.Map({
    container:'map',
    style: {
      version:8,
      sources:{ osm:{ type:'raster', tiles:['https://tile.openstreetmap.org/{z}/{x}/{y}.png'], tileSize:256, maxzoom:19 } },
      layers:[ { id:'osm', type:'raster', source:'osm' } ]
    },
    center:[-80.0,40.5], zoom:8
  });

  map.addControl(new maplibregl.AttributionControl({ compact: true }));
  window.map = map;

  window.latLonToTile = function(lat, lon, zoom){
    const latRad = lat * Math.PI / 180;
    const n = Math.pow(2, zoom);
    const x = Math.floor((lon + 180) / 360 * n);
    const y = Math.floor((1 - Math.log(Math.tan(latRad) + 1 / Math.cos(latRad)) / Math.PI) / 2 * n);
    return { z: zoom, x, y };
  };
  window.getTileAtCenter = function(z){
    const c = map.getCenter(); return window.latLonToTile(c.lat, c.lng, z);
  };

  const REQUIRED_ZOOM = 10;
  (document.getElementById('zoomReqText')||{}).textContent = String(REQUIRED_ZOOM);

  let inspectOn = false;
  const inspectBtn = document.getElementById('inspectBtn');

  inspectBtn.addEventListener('click', ()=>{
    inspectOn=!inspectOn;
    inspectBtn.classList.toggle('active', inspectOn);
    map.getCanvas().style.cursor = inspectOn ? 'crosshair' : '';
    clearHighlight();
  });

  let drawing=false, sketchCoords=[], hasLine=false;
  let usgsLayerSpecs = [];

  function clearHighlight(){
    const src = map.getSource('inspect-highlight');
    if (src) src.setData({ type:'FeatureCollection', features:[] });
  }
  function visibleLayers(ids){
    return ids.filter(id => map.getLayer(id) && map.getLayoutProperty(id,'visibility') !== 'none');
  }

  // Prefer osm_id from your overlay; also check other common keys
  function resolveId(p){
    const keys = ['osm_id','way_id','@id','canonical_id','osm_way_id','osmId','feature_id','id'];
    for (const k of keys){
      const v = p && p[k];
      if (v !== undefined && v !== null && String(v).trim() !== '') return String(v);
    }
    return '—';
  }

  function buildPopupTable(rows){
    const head = `
      <table style="border-collapse:collapse;min-width:360px">
        <thead>
          <tr>
            <th style="text-align:left;padding:4px 6px;border-bottom:1px solid #ddd;">Layer</th>
            <th style="text-align:left;padding:4px 6px;border-bottom:1px solid #ddd;">ID</th>
            <th style="text-align:left;padding:4px 6px;border-bottom:1px solid #ddd;">Highway</th>
            <th style="text-align:left;padding:4px 6px;border-bottom:1px solid #ddd;">Speed</th>
            <th style="text-align:left;padding:4px 6px;border-bottom:1px solid #ddd;">Traffic</th>
            <th style="text-align:left;padding:4px 6px;border-bottom:1px solid #ddd;">Surface</th>
            <th style="text-align:left;padding:4px 6px;border-bottom:1px solid #ddd;">Name</th>
          </tr>
        </thead>
        <tbody>`;
    const body = rows.map(r => `
      <tr>
        <td style="padding:4px 6px;border-bottom:1px solid #eee;">${r.layer || '—'}</td>
        <td style="padding:4px 6px;border-bottom:1px solid #eee;">${r.id || '—'}</td>
        <td style="padding:4px 6px;border-bottom:1px solid #eee;">${r.highway || '—'}</td>
        <td style="padding:4px 6px;border-bottom:1px solid #eee;">${r.speed || '—'}</td>
        <td style="padding:4px 6px;border-bottom:1px solid #eee;">${r.traffic || '—'}</td>
        <td style="padding:4px 6px;border-bottom:1px solid #eee;">${r.surface || '—'}</td>
        <td style="padding:4px 6px;border-bottom:1px solid #eee;">${r.name || '—'}</td>
      </tr>`).join('');
    return head + body + `</tbody></table>`;
  }

  function escapeHtml(value){
    return String(value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function formatValue(value){
    if (value === null || value === undefined) return '';
    if (typeof value === 'number' && Number.isFinite(value)) return value.toLocaleString();
    return String(value);
  }

  function formatWmsAttributes(props){
    const preferred = [
      'name','Name','GNIS_NAME','FEATURE_NAME','FEATURE',
      'FType','FTYPE','TYPE','type','CLASS','class',
      'STATE_NAME','COUNTY_NAME','STUSPS','UNIT_NAME',
      'GNIS_ID','ID','OBJECTID','FID'
    ];
    const rows = [];
    const seen = new Set();
    preferred.forEach((key)=>{
      if (!props || seen.has(key)) return;
      const value = props[key];
      if (value === null || value === undefined || String(value).trim() === '') return;
      rows.push([key, value]);
      seen.add(key);
    });
    if (!rows.length && props) {
      Object.entries(props).slice(0, 6).forEach(([key, value])=>{
        rows.push([key, value]);
      });
    }
    return rows;
  }

  function buildWmsPopup(rows){
    const body = rows.map((row)=>{
      const attrs = formatWmsAttributes(row.attributes);
      const lines = attrs.map(([k, v])=>`<div><strong>${escapeHtml(k)}:</strong> ${escapeHtml(formatValue(v))}</div>`).join('');
      return `
        <div style="margin-bottom:10px">
          <div style="font-weight:700;margin-bottom:4px">${escapeHtml(row.layerLabel)}</div>
          ${lines || '<div>Feature found</div>'}
        </div>`;
    }).join('');
    return `<div style="min-width:260px">${body}</div>`;
  }

  function lngLatToMercator(lng, lat){
    const x = lng * 20037508.34 / 180;
    let y = Math.log(Math.tan((90 + lat) * Math.PI / 360)) / (Math.PI / 180);
    y = y * 20037508.34 / 180;
    return [x, y];
  }

  function cleanUrl(value){
    return (value || '').replace(/\/+$/, '');
  }

  function wmtsTileUrl(baseUrl, layerName){
    const base = cleanUrl(baseUrl);
    return `${base}/tile/1.0.0/${layerName}/default/default028mm/{z}/{y}/{x}`;
  }

  function wmsTileUrl(baseUrl, layers){
    const base = cleanUrl(baseUrl);
    return `${base}?service=WMS&request=GetMap&version=1.3.0&layers=${layers}&styles=&format=image/png&transparent=true&width=256&height=256&crs=EPSG:3857&bbox={bbox-epsg-3857}`;
  }

  function buildWmsFeatureInfoUrl(spec, e){
    const canvas = map.getCanvas();
    const scale = canvas.clientWidth ? (canvas.width / canvas.clientWidth) : 1;
    const i = Math.round(e.point.x * scale);
    const j = Math.round(e.point.y * scale);
    const bounds = map.getBounds();
    const sw = bounds.getSouthWest();
    const ne = bounds.getNorthEast();
    const min = lngLatToMercator(sw.lng, sw.lat);
    const max = lngLatToMercator(ne.lng, ne.lat);
    const bbox = `${min[0]},${min[1]},${max[0]},${max[1]}`;
    const layers = encodeURIComponent(spec.wmsLayers);
    const base = cleanUrl(spec.url);
    return `${base}?service=WMS&request=GetFeatureInfo&version=1.3.0&layers=${layers}&query_layers=${layers}&styles=&info_format=application/json&feature_count=5&crs=EPSG:3857&bbox=${bbox}&width=${canvas.width}&height=${canvas.height}&i=${i}&j=${j}`;
  }

  async function fetchWmsIdentify(spec, e){
    if (!spec || !spec.url || !spec.wmsLayers) return [];
    try{
      const url = buildWmsFeatureInfoUrl(spec, e);
      const r = await fetch(url);
      if (!r.ok) return [];
      const data = await r.json();
      const feats = data.features || (data.featureCollection && data.featureCollection.features) || [];
      return feats.map((f)=>({
        layerLabel: spec.label,
        attributes: f.properties || f.attributes || {}
      }));
    }catch(_){
      return [];
    }
  }

  map.on('load', () => {
    setStatus('map ready');

    // highlight + sketch helpers
    map.addSource('inspect-highlight', { type:'geojson', data:{ type:'FeatureCollection', features:[] } });
    map.addLayer({ id:'inspect-highlight', type:'line', source:'inspect-highlight',
      paint:{ 'line-color':'#00bcd4', 'line-width':4, 'line-opacity':0.95 } });

    map.addSource('sketch', { type:'geojson', data:{ type:'FeatureCollection', features:[] } });
    map.addLayer({ id:'sketch-line', type:'line', source:'sketch',
      paint:{ 'line-color':'#2563eb','line-width':['interpolate',['linear'],['zoom'],5,2,12,4,16,6] } });

    const usgsMsgEl = document.getElementById('usgsMsg');
    const usgsAttribution = 'USGS The National Map';
    const usgsLayerDefs = [
      {
        key:'naip',
        label:'NAIP Imagery',
        type:'wmts',
        url:'https://basemap.nationalmap.gov/arcgis/rest/services/USGSImageryOnly/MapServer/WMTS',
        wmtsLayer:'USGSImageryOnly',
        checkboxId:'chkUsgsNaip',
        minzoom:0,
        maxzoom:19,
        opacity:1
      },
      {
        key:'topo',
        label:'USGS Topo',
        type:'wmts',
        url:'https://basemap.nationalmap.gov/arcgis/rest/services/USGSTopo/MapServer/WMTS',
        wmtsLayer:'USGSTopo',
        checkboxId:'chkUsgsTopo',
        minzoom:0,
        maxzoom:19,
        opacity:0.95
      },
      {
        key:'hillshade',
        label:'Hillshade',
        type:'wmts',
        url:'https://basemap.nationalmap.gov/arcgis/rest/services/USGSHillshadeOnly/MapServer/WMTS',
        wmtsLayer:'USGSHillshadeOnly',
        checkboxId:'chkUsgsHillshade',
        minzoom:0,
        maxzoom:16,
        opacity:0.7
      },
      {
        key:'hydro',
        label:'Hydrography',
        type:'wms',
        url:'https://hydro.nationalmap.gov/arcgis/services/nhd/MapServer/WmsServer',
        wmsLayers:'0,1,2,3',
        checkboxId:'chkUsgsHydro',
        minzoom:6,
        maxzoom:19,
        opacity:0.85,
        identify:true
      },
      {
        key:'transport',
        label:'Transportation',
        type:'wms',
        url:'https://carto.nationalmap.gov/arcgis/services/transportation/MapServer/WmsServer',
        wmsLayers:'0,1,2,3,4',
        checkboxId:'chkUsgsTransport',
        minzoom:7,
        maxzoom:19,
        opacity:0.85,
        identify:true
      },
      {
        key:'boundaries',
        label:'Boundaries',
        type:'wms',
        url:'https://carto.nationalmap.gov/arcgis/services/boundaries/MapServer/WmsServer',
        wmsLayers:'0,1,2,3',
        checkboxId:'chkUsgsBoundaries',
        minzoom:5,
        maxzoom:19,
        opacity:0.8,
        identify:true
      },
      {
        key:'geonames',
        label:'Geographic Names',
        type:'wms',
        url:'https://carto.nationalmap.gov/arcgis/services/geonames/MapServer/WmsServer',
        wmsLayers:'0,1,2',
        checkboxId:'chkUsgsNames',
        minzoom:8,
        maxzoom:19,
        opacity:1,
        identify:false
      }
    ];

    usgsLayerSpecs = usgsLayerDefs.map((spec)=>({
      ...spec,
      id:`usgs-${spec.key}`,
      sourceId:`usgs-${spec.key}-src`,
      layerId:`usgs-${spec.key}-layer`,
      attribution: spec.attribution || usgsAttribution
    }));

    function ensureUsgsLayer(spec){
      if (map.getSource(spec.sourceId)) return true;
      const tiles = spec.type === 'wmts'
        ? [wmtsTileUrl(spec.url, spec.wmtsLayer)]
        : [wmsTileUrl(spec.url, spec.wmsLayers)];
      map.addSource(spec.sourceId, {
        type:'raster',
        tiles,
        tileSize:256,
        attribution: spec.attribution,
        maxzoom: spec.maxzoom
      });
      map.addLayer({
        id: spec.layerId,
        type:'raster',
        source: spec.sourceId,
        paint:{ 'raster-opacity': spec.opacity ?? 1 },
        layout:{ visibility:'none' },
        minzoom: spec.minzoom,
        maxzoom: spec.maxzoom
      });
      return true;
    }

    function isUsgsLayerVisible(spec){
      return map.getLayer(spec.layerId) && map.getLayoutProperty(spec.layerId, 'visibility') !== 'none';
    }

    function syncUsgsLayers(){
      let active = 0;
      usgsLayerSpecs.forEach((spec)=>{
        const checkbox = document.getElementById(spec.checkboxId);
        const on = !!(checkbox && checkbox.checked);
        if (on) ensureUsgsLayer(spec);
        if (map.getLayer(spec.layerId)) {
          map.setLayoutProperty(spec.layerId, 'visibility', on ? 'visible' : 'none');
          if (on) active += 1;
        }
      });
      if (usgsMsgEl) {
        usgsMsgEl.textContent = active
          ? 'USGS layers active. Toggle off to hide.'
          : 'USGS layers stream from nationalmap.gov and are off by default.';
      }
    }

    usgsLayerSpecs.forEach((spec)=>{
      const el = document.getElementById(spec.checkboxId);
      if (el) el.addEventListener('change', syncUsgsLayers);
    });
    syncUsgsLayers();

    // pmtiles protocol
    if (!window.pmtiles) {
      (document.getElementById('surfaceMsg')||{}).textContent='pmtiles.js missing';
      return;
    }
    const protocol = new pmtiles.Protocol();
    maplibregl.addProtocol('pmtiles', protocol.tile);

    const surfaceMsgEl = document.getElementById('surfaceMsg');
    const crashMsgEl = document.getElementById('crashMsg');
    const trafficCheckboxId = 'chkTraffic';
    const speedCheckboxId = 'chkSpeed';

    const setSurfaceMessage = (text) => {
      if (!surfaceMsgEl) return;
      surfaceMsgEl.textContent = text;
    };

    function pmtilesUrlOrWarn(name, fallback) {
      const url = pmtilesAsset(name);
      if (!url) {
        setSurfaceMessage(fallback);
        throw new Error(`Missing PMTiles URL for ${name}`);
      }
      return url;
    }

    let paLayerReady = false;
    const ensurePaLayer = (active) => {
      if (!active && !paLayerReady) return false;
      if (paLayerReady) return true;
      try {
        const paUnpavedUrl = pmtilesUrlOrWarn('paunpavedgravel.pmtiles', 'Unable to load the PA surface tiles.');
        map.addSource('paunpaved', { type:'vector', url: paUnpavedUrl });
        map.addLayer({
          id:'paunpaved-layer', type:'line', source:'paunpaved', 'source-layer':'gravel',
          paint:{ 'line-color':'#b87333',
                  'line-width':['interpolate',['linear'],['zoom'],6,1.8,12,3.2,16,5.0],
                  'line-opacity':0.95 },
          layout:{ 'line-cap':'round','line-join':'round', 'visibility':'none' }
        });
        map.moveLayer('paunpaved-layer');
        paLayerReady = true;
      } catch (err) {
        console.warn('paunpaved add failed', err);
      }
      return paLayerReady;
    };

    const layerDefaults = {
      'traffic-layer': 0.85,
      'speed-layer': 0.9
    };
    const ghostConfig = {
      ghostTraffic: { layer: 'traffic-layer', opacity: layerDefaults['traffic-layer'] },
      ghostSpeed: { layer: 'speed-layer', opacity: layerDefaults['speed-layer'] },
    };
    const applyGhostState = (ctrlId) => {
      const spec = ghostConfig[ctrlId];
      if (!spec) return;
      const ctrl = document.getElementById(ctrlId);
      const ghostOn = ctrl ? ctrl.checked : false;
      const currentOpacity = ghostOn ? 0 : spec.opacity;
      if (map.getLayer(spec.layer)) {
        map.setPaintProperty(spec.layer, 'line-opacity', currentOpacity);
      }
    };
    let trafficLayerReady = false;
    const ensureTrafficLayer = (active) => {
      if (!active && !trafficLayerReady) return false;
      if (trafficLayerReady) return true;
      try {
        const trafficUrl = pmtilesUrlOrWarn('traffic.pmtiles', 'Unable to load the traffic tiles.');
        map.addSource('traffic', { type:'vector', url: trafficUrl });
        map.addLayer({
          id:'traffic-layer',
          type:'line',
          source:'traffic',
          'source-layer':'traffic',
          minzoom:0,
          maxzoom:24,
          paint:{
            'line-width':['interpolate',['linear'],['zoom'],8,1.5,14,3.5],
            'line-color':['interpolate',['linear'],['get','CUR_AADT'],0,'#d9f99d',5000,'#fef08a',15000,'#fb923c',30000,'#ef4444'],
            'line-opacity':layerDefaults['traffic-layer']
          },
          layout:{ 'visibility':'none', 'line-cap':'round', 'line-join':'round' }
        });
        trafficLayerReady = true;
      } catch (err) {
        console.warn('traffic add failed', err);
      }
      return trafficLayerReady;
    };

    let speedLayerReady = false;
    const ensureSpeedLayer = (active) => {
      if (!active && !speedLayerReady) return false;
      if (speedLayerReady) return true;
      try {
        const speedUrl = pmtilesUrlOrWarn('speed.pmtiles', 'Unable to load the speed tiles.');
        map.addSource('speed', { type:'vector', url: speedUrl });
        map.addLayer({
          id:'speed-layer',
          type:'line',
          source:'speed',
          'source-layer':'speed',
          minzoom:0,
          maxzoom:24,
          paint:{
            'line-width':['interpolate',['linear'],['zoom'],8,1.3,14,3],
            'line-color':['interpolate', ['linear'], ['get','SPEED'], 5, '#a7f3d0', 15, '#22d3ee', 30, '#ffd60a', 45, '#fb923c'],
            'line-opacity':layerDefaults['speed-layer']
          },
          layout:{ 'visibility':'none', 'line-cap':'round', 'line-join':'round' }
        });
        speedLayerReady = true;
      } catch (err) {
        console.warn('speed add failed', err);
      }
      return speedLayerReady;
    };

    const crashLayerId = (year)=>`crash-${year}-layer`;
    const crashSourceId = (year)=>`crash-${year}`;
    const crashCheckboxId = (year)=>`chkCrash${year}`;
    const crashLayerReady = new Set();
    const crashHoverReady = new Set();
    const crashClickReady = new Set();
    const crashFilterEls = {
      dow: ()=>document.getElementById('crashFilterDow'),
      month: ()=>document.getElementById('crashFilterMonth'),
      fatal: ()=>document.getElementById('crashFilterFatal'),
      injury: ()=>document.getElementById('crashFilterInjury'),
      bicycle: ()=>document.getElementById('crashFilterBicycle'),
      ped: ()=>document.getElementById('crashFilterPed')
    };
    const buildCrashFilter = () => {
      const filters = ['all'];
      const dowVal = crashFilterEls.dow()?.value || '';
      const monthVal = crashFilterEls.month()?.value || '';
      if (dowVal) filters.push(['==', ['to-number', ['get', 'DAY_OF_WEEK']], Number(dowVal)]);
      if (monthVal) filters.push(['==', ['to-number', ['get', 'CRASH_MONTH']], Number(monthVal)]);
      if (crashFilterEls.fatal()?.checked) filters.push(['==', ['to-number', ['get', 'FATAL']], 1]);
      if (crashFilterEls.injury()?.checked) filters.push(['>', ['to-number', ['get', 'INJURY_COUNT']], 0]);
      if (crashFilterEls.bicycle()?.checked) filters.push(['==', ['to-number', ['get', 'BICYCLE']], 1]);
      if (crashFilterEls.ped()?.checked) filters.push(['==', ['to-number', ['get', 'PEDESTRIAN']], 1]);
      return filters.length > 1 ? filters : null;
    };
    const applyCrashFilters = () => {
      const filter = buildCrashFilter();
      crashYears.forEach((year)=>{
        const layerId = crashLayerId(year);
        if (!map.getLayer(layerId)) return;
        map.setFilter(layerId, filter);
      });
    };
    const ensureCrashLayer = (year, active) => {
      if (!active && !crashLayerReady.has(year)) return false;
      if (crashLayerReady.has(year)) return true;
      try {
        const crashUrl = pmtilesUrlOrWarn(`crash_${year}.pmtiles`, 'Unable to load the crash tiles.');
        map.addSource(crashSourceId(year), { type:'vector', url: crashUrl });
        map.addLayer({
          id: crashLayerId(year),
          type:'circle',
          source: crashSourceId(year),
          'source-layer':'crashes',
          paint:{
            'circle-color': crashColorByYear[year] || '#e11d48',
            'circle-opacity':0.6,
            'circle-radius':[
              'interpolate',['linear'],['zoom'],
              0,2,
              9,2,
              11,3,
              12,4,
              24,4
            ]
          },
          layout:{ 'visibility':'none' }
        });
        applyCrashFilters();
        crashLayerReady.add(year);
        if (!crashHoverReady.has(year)) {
          attachHoverInfo(crashLayerId(year), crashHoverHtml);
          crashHoverReady.add(year);
        }
        if (!crashClickReady.has(year)) {
          map.on('click', crashLayerId(year), (e)=>{
            const feat = e.features && e.features[0];
            if (!feat) return;
            new maplibregl.Popup({ closeOnClick: true, maxWidth: '520px' })
              .setLngLat(e.lngLat)
              .setHTML(crashHoverHtml(feat.properties || {}))
              .addTo(map);
          });
          crashClickReady.add(year);
        }
      } catch (err) {
        console.warn(`crash_${year} add failed`, err);
      }
      return crashLayerReady.has(year);
    };

    // ===== OSM-ID OVERLAY (PA) — your pa_roads1.pmtiles =====
    try{
      const idOverlayUrl = pmtilesAsset('pa_roads1.pmtiles');
      map.addSource('pa_roads1_src', { type:'vector', url: idOverlayUrl });
      // transparent hit layer for the overlay
      map.addLayer({
        id: 'pa-roads1-hit',
        type: 'line',
        source: 'pa_roads1_src',
        'source-layer': 'roads',   // must match tippecanoe -l roads
        paint: { 'line-opacity': 0, 'line-width': 12 },
        layout: { 'line-cap':'round','line-join':'round' }
      });
      map.moveLayer('pa-roads1-hit');
    }catch(e){ console.warn('pa_roads1 overlay add failed', e); }

    const vis = (on)=> on ? 'visible' : 'none';
    function syncChecks(){
      const checked = (id)=> !!(document.getElementById(id) && document.getElementById(id).checked);
      const paOn = checked('chkPAUnpaved');
      if (paOn) ensurePaLayer(true);
      if (map.getLayer('paunpaved-layer')) {
        map.setLayoutProperty('paunpaved-layer','visibility', vis(paOn));
      }
      const trafficOn = checked(trafficCheckboxId);
      if (trafficOn) ensureTrafficLayer(true);
      if (map.getLayer('traffic-layer')) {
        map.setLayoutProperty('traffic-layer','visibility', vis(trafficOn));
        applyGhostState('ghostTraffic');
      }
      const speedOn = checked(speedCheckboxId);
      if (speedOn) ensureSpeedLayer(true);
      if (map.getLayer('speed-layer')) {
        map.setLayoutProperty('speed-layer','visibility', vis(speedOn));
        applyGhostState('ghostSpeed');
      }
      let crashOn = false;
      crashYears.forEach((year)=>{
        const yearOn = checked(crashCheckboxId(year));
        if (yearOn) ensureCrashLayer(year, true);
        if (map.getLayer(crashLayerId(year))) {
          map.setLayoutProperty(crashLayerId(year),'visibility', vis(yearOn));
        }
        crashOn = crashOn || yearOn;
      });
      if (surfaceMsgEl) {
        if (paOn || trafficOn || speedOn) {
          setSurfaceMessage('Surface or traffic layers active. Uncheck to hide.');
        } else {
          setSurfaceMessage('Surface layers are disabled by default — toggle any checkbox to load a layer.');
        }
      }
      if (crashMsgEl) {
        crashMsgEl.textContent = crashOn
          ? 'Crash layer active. Toggle off to hide.'
          : 'Crash layer is disabled by default — toggle to load.';
      }
    }
    ['chkPAUnpaved', trafficCheckboxId, speedCheckboxId].forEach(id=>{
      const el = document.getElementById(id);
      if (el) el.addEventListener('change', syncChecks);
    });
    crashYears.forEach((year)=>{
      const el = document.getElementById(crashCheckboxId(year));
      if (el) el.addEventListener('change', syncChecks);
    });
    const crashFilterToggle = document.getElementById('crashFilterToggle');
    const crashFilterBody = document.getElementById('crashFilterBody');
    if (crashFilterToggle && crashFilterBody) {
      crashFilterToggle.addEventListener('click', ()=>{
        const collapsed = crashFilterBody.style.display === 'none';
        crashFilterBody.style.display = collapsed ? '' : 'none';
        crashFilterToggle.textContent = collapsed ? 'Hide' : 'Show';
        crashFilterToggle.setAttribute('aria-expanded', collapsed ? 'true' : 'false');
      });
    }
    Object.values(crashFilterEls).forEach((getter)=>{
      const el = getter();
      if (el) el.addEventListener('change', applyCrashFilters);
    });
    const crashFilterClear = document.getElementById('crashFilterClear');
    if (crashFilterClear) {
      crashFilterClear.addEventListener('click', ()=>{
        if (crashFilterEls.dow()) crashFilterEls.dow().value = '';
        if (crashFilterEls.month()) crashFilterEls.month().value = '';
        if (crashFilterEls.fatal()) crashFilterEls.fatal().checked = false;
        if (crashFilterEls.injury()) crashFilterEls.injury().checked = false;
        if (crashFilterEls.bicycle()) crashFilterEls.bicycle().checked = false;
        if (crashFilterEls.ped()) crashFilterEls.ped().checked = false;
        applyCrashFilters();
      });
    }
    const crashSelectAllBtn = document.getElementById('crashSelectAll');
    if (crashSelectAllBtn) {
      crashSelectAllBtn.addEventListener('click', ()=>{
        crashYears.forEach((year)=>{
          const el = document.getElementById(crashCheckboxId(year));
          if (el) el.checked = true;
        });
        syncChecks();
      });
    }
    Object.keys(ghostConfig).forEach(ctrlId=>{
      const el = document.getElementById(ctrlId);
      if (!el) return;
      el.addEventListener('change', ()=>{
        if (ctrlId === 'ghostTraffic') ensureTrafficLayer(true);
        if (ctrlId === 'ghostSpeed') ensureSpeedLayer(true);
        applyGhostState(ctrlId);
      });
    });
    syncChecks();
    // Warm up the traffic/speed sources so checking the box is snappier.
    map.once('idle', ()=>{
      ensureTrafficLayer(true);
      ensureSpeedLayer(true);
      if (map.getLayer('traffic-layer')) {
        map.setLayoutProperty('traffic-layer','visibility','none');
      }
      if (map.getLayer('speed-layer')) {
        map.setLayoutProperty('speed-layer','visibility','none');
      }
    });
  });

  const hoverPopup = new maplibregl.Popup({
    closeButton:false,
    closeOnClick:false,
    offset:[0,-12],
  });

  function attachHoverInfo(layerId, format){
    map.on('mousemove', layerId, (e)=>{
      const feature = e.features && e.features[0];
      if (!feature) return;
      map.getCanvas().style.cursor='pointer';
      const html = format(feature.properties||{});
      hoverPopup.setLngLat(e.lngLat).setHTML(html).addTo(map);
    });
    map.on('mouseleave', layerId, ()=>{
      map.getCanvas().style.cursor='';
      hoverPopup.remove();
    });
  }

  attachHoverInfo('traffic-layer', (props)=>{
    const lines = [];
    if (props.highway) lines.push(`<strong>Highway:</strong> ${props.highway}`);
    if (props.CUR_AADT) lines.push(`<strong>AADT:</strong> ${props.CUR_AADT.toLocaleString()} vehicles`);
    if (props.name) lines.push(`<strong>Name:</strong> ${props.name}`);
    return lines.length ? lines.join('<br>') : 'Traffic data';
  });
  attachHoverInfo('speed-layer', (props)=>{
    const lines = [];
    if (props.highway) lines.push(`<strong>Highway:</strong> ${props.highway}`);
    if (props.SPEED) lines.push(`<strong>Speed:</strong> ${props.SPEED} mph`);
    if (props.name) lines.push(`<strong>Name:</strong> ${props.name}`);
    return lines.length ? lines.join('<br>') : 'Speed data';
  });

  function crashHoverHtml(props){
    const crashLabelMap = {
      FATAL: 'Fatal',
      INJURY_COUNT: 'Injuries',
      BICYCLE: 'Bicycle Involved',
      BICYCLE_DEATH_COUNT: 'Bicycle Deaths',
      PEDESTRIAN: 'Pedestrian Involved',
      PED_DEATH_COUNT: 'Pedestrian Deaths',
      COLLISION_TYPE: 'Collision Type',
      ROAD_CONDITION: 'Road Condition',
      SPEED_LIMIT: 'Speed Limit'
    };
    const sections = [
      {
        title:'Severity',
        keys:['FATAL','INJURY_COUNT']
      },
      {
        title:'Cyclist / Pedestrian',
        keys:['BICYCLE','BICYCLE_DEATH_COUNT','PEDESTRIAN','PED_DEATH_COUNT']
      },
      {
        title:'Crash context',
        keys:['COLLISION_TYPE','ROAD_CONDITION','SPEED_LIMIT']
      }
    ];

    const blocks = [];
    sections.forEach((section)=>{
      const rows = [];
      section.keys.forEach((key)=>{
        const value = props ? props[key] : undefined;
        if (value === null || value === undefined || String(value).trim() === '') return;
        const label = crashLabelMap[key] || key.replace(/_/g, ' ').replace(/\b\w/g, (m)=>m.toUpperCase());
        rows.push(`<div><strong>${escapeHtml(label)}:</strong> ${escapeHtml(formatValue(value))}</div>`);
      });
      if (!rows.length) return;
      blocks.push(`
        <div style="margin-bottom:10px">
          <div style="font-weight:700;margin-bottom:4px">${escapeHtml(section.title)}</div>
          ${rows.join('')}
        </div>`);
    });

    if (!blocks.length) return 'Crash record';
    let html = `<div style="min-width:260px;max-height:320px;overflow:auto;padding-right:6px">${blocks.join('')}</div>`;
    const year = props && props.CRASH_YEAR;
    const month = props && props.CRASH_MONTH;
    const dow = props && props.DAY_OF_WEEK;
    const hour = props && props.HOUR_OF_DAY;
    const arrival = props && props.ARRIVAL_TM;
    const dispatch = props && props.DISPATCH_TM;
    const monthNames = ['January','February','March','April','May','June','July','August','September','October','November','December'];
    let dateLabel = '';
    if (year && month) {
      const mIdx = Number(month) - 1;
      const mName = monthNames[mIdx] || month;
      dateLabel = `${mName} ${year}`;
    } else if (year) {
      dateLabel = String(year);
    }
    let timeLabel = '';
    const formatHour = (hNum, mNum = 0)=>{
      if (Number.isNaN(hNum)) return '';
      const h12 = ((hNum + 11) % 12) + 1;
      const ampm = hNum >= 12 ? 'PM' : 'AM';
      const mm = String(mNum).padStart(2, '0');
      return `${h12}:${mm} ${ampm}`;
    };
    if (hour !== null && hour !== undefined && String(hour).trim() !== '') {
      const hNum = Number(hour);
      if (!Number.isNaN(hNum)) {
        timeLabel = formatHour(hNum, 0);
      }
    }
    if (!timeLabel) {
      const fallback = arrival ?? dispatch;
      if (fallback !== null && fallback !== undefined && String(fallback).trim() !== '') {
        const raw = String(fallback).replace(/\D/g, '').padStart(4, '0').slice(-4);
        const hNum = Number(raw.slice(0, 2));
        const mNum = Number(raw.slice(2, 4));
        if (!Number.isNaN(hNum) && !Number.isNaN(mNum)) {
          timeLabel = formatHour(hNum, mNum);
        }
      }
    }
    let dowLabel = '';
    if (dow !== null && dow !== undefined && String(dow).trim() !== '') {
      const dNum = Number(dow);
      const dowNames = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
      dowLabel = (!Number.isNaN(dNum) && dowNames[dNum - 1]) ? dowNames[dNum - 1] : String(dow);
    }
    if (dateLabel || timeLabel || dowLabel) {
      const pieces = [];
      if (dateLabel) pieces.push(dateLabel);
      if (dowLabel) pieces.push(dowLabel);
      if (timeLabel) pieces.push(`at ${timeLabel}`);
      const line = pieces.join(', ');
      html = `<div style="font-weight:700;margin-bottom:8px">${escapeHtml(line)}</div>` + html;
    }
    return html;
  }

  async function queryWmsAtPoint(e){
    if (!usgsLayerSpecs.length) return [];
    const zoom = map.getZoom();
    const active = usgsLayerSpecs.filter((spec)=>{
      if (spec.type !== 'wms' || !spec.identify) return false;
      if (zoom < spec.minzoom) return false;
      return map.getLayer(spec.layerId) && map.getLayoutProperty(spec.layerId, 'visibility') !== 'none';
    });
    if (!active.length) return [];
    const results = [];
    for (const spec of active) {
      const feats = await fetchWmsIdentify(spec, e);
      feats.slice(0, 2).forEach((f)=>results.push(f));
      if (results.length >= 6) break;
    }
    return results;
  }


  // -------- Single-nearest feature inspector --------
  map.on('click', async (e) => {
    if (!inspectOn) return;

    // distance from click to a line feature (meters)
    function distToLine(feat, lngLat) {
      try {
        const pt = turf.point([lngLat.lng, lngLat.lat]);
        const n = turf.nearestPointOnLine(feat, pt, { units: 'meters' });
        return n.properties.dist || 1e12;
      } catch {
        return 1e12;
      }
    }

    // 1) Prefer the ID overlay, but allow traffic/speed too
    const inspectLayers = ['pa-roads1-hit', 'traffic-layer', 'speed-layer', ...crashYears.map(crashLayerId)];
    let feats = map.queryRenderedFeatures(e.point, { layers: inspectLayers });

    if (!feats.length) {
      clearHighlight();
      const wmsRows = await queryWmsAtPoint(e);
      if (!wmsRows.length) return;
      new maplibregl.Popup({ closeOnClick: true, maxWidth: '520px' })
        .setLngLat(e.lngLat)
        .setHTML(buildWmsPopup(wmsRows))
        .addTo(map);
      return;
    }

    // choose the single nearest feature to the click
    const best = feats.reduce((a, b) => {
      const da = distToLine(a, e.lngLat);
      const db = distToLine(b, e.lngLat);
      return db < da ? b : a;
    });

    const p = best.properties || {};

    if (best.layer && crashYears.some((year)=>best.layer.id === crashLayerId(year))) {
      new maplibregl.Popup({ closeOnClick: true, maxWidth: '520px' })
        .setLngLat(e.lngLat)
        .setHTML(crashHoverHtml(p))
        .addTo(map);
      return;
    }
    const row = [{
      layer: best.layer ? best.layer.id : '—',
      id: resolveId(p),
      highway: p.highway || p.class || p.subclass || '',
      surface: p.surface || '',
      name: p.name || p.ref || '',
      speed: p.SPEED ? `${p.SPEED} mph` : '',
      traffic: p.CUR_AADT ? `${p.CUR_AADT.toLocaleString()} vehicles` : ''
    }];

    // highlight exactly that feature
    map.getSource('inspect-highlight')?.setData({
      type:'FeatureCollection',
      features:[{ type:'Feature', geometry: best.geometry, properties:{} }]
    });

    try {
      if (!window.__loggedInspectPropsOnce) {
        console.log('[Inspect] sample props:', p);
        window.__loggedInspectPropsOnce = true;
      }
    } catch (_) {}

    new maplibregl.Popup({ closeOnClick: true, maxWidth: '520px' })
      .setLngLat(e.lngLat)
      .setHTML(buildPopupTable(row))
      .addTo(map);
  });

  // -------- Draw UI (unchanged) --------
  const drawFab = document.getElementById('drawFab');
  function startDrawing(){ drawing=true; sketchCoords=[]; map.dragPan.disable(); map.getCanvas().style.cursor='crosshair'; drawFab.classList.add('active'); drawFab.setAttribute('aria-pressed','true'); (document.getElementById('drawState')||{}).textContent="Click then press & hold to draw; release to finish."; }
  function stopDrawing(){ drawing=false; map.dragPan.enable(); map.getCanvas().style.cursor=''; drawFab.classList.remove('active'); drawFab.setAttribute('aria-pressed','false'); updateSketch(); }
  drawFab.addEventListener('click', ()=>{ drawing ? stopDrawing() : startDrawing(); });

  function updateSketch(){
    const src = map.getSource('sketch'); if(!src) return;
    if (sketchCoords.length < 2){
      src.setData({ type:'FeatureCollection', features:[] });
      hasLine=false; const lf=document.getElementById('lenField'); if(lf) lf.value='—';
      const sb=document.getElementById('submitDrawBtn'); if(sb) sb.disabled=true;
      return;
    }
    const feat={ type:'Feature', geometry:{ type:'LineString', coordinates:sketchCoords }, properties:{} };
    src.setData({ type:'FeatureCollection', features:[feat] });
    hasLine=true;
    const m=Math.round(turf.length(feat,{units:'kilometers'})*1000);
    const lf=document.getElementById('lenField'); if(lf) lf.value = `${m.toLocaleString()} m`;
    const sb=document.getElementById('submitDrawBtn'); if(sb) sb.disabled=false;
    (document.getElementById('drawState')||{}).textContent="Line ready — Submit when done.";
  }

  map.on('mousedown', (e)=>{
    if(!drawing) return;
    sketchCoords=[[e.lngLat.lng, e.lngLat.lat]]; updateSketch();
    function move(ev){
      const ll=ev.lngLat, last=sketchCoords[sketchCoords.length-1];
      if (Math.abs(ll.lng-last[0])>1e-5 || Math.abs(ll.lat-last[1])>1e-5) sketchCoords.push([ll.lng,ll.lat]);
      updateSketch();
    }
    function up(){ map.off('mousemove', move); map.off('mouseup', up); stopDrawing(); }
    map.on('mousemove', move); map.on('mouseup', up);
  });

  map.on('touchstart', (e)=>{
    if(!drawing) return;
    const t=e.points?.[0]||e.originalEvent.touches[0]; if(!t) return;
    const ll=map.unproject([t.clientX,t.clientY]); sketchCoords=[[ll.lng,ll.lat]]; updateSketch();
    function move(ev){
      const tp=ev.points?.[0]||ev.originalEvent.touches[0]; if(!tp) return;
      const ll2=map.unproject([tp.clientX,tp.clientY]);
      const last=sketchCoords[sketchCoords.length-1];
      if (Math.abs(ll2.lng-last[0])>1e-5 || Math.abs(ll2.lat-last[1])>1e-5) sketchCoords.push([ll2.lng,ll2.lat]);
      updateSketch();
    }
    function end(){ map.off('touchmove', move); map.off('touchend', end); stopDrawing(); }
    map.on('touchmove', move); map.on('touchend', end);
  });

  document.getElementById('clearDrawBtn').addEventListener('click', ()=>{
    sketchCoords=[]; updateSketch(); const d=document.getElementById('drawNote'); if(d) d.value=''; const s=document.getElementById('drawState'); if(s) s.textContent="Click ✎ then press & hold to draw.";
  });

  document.getElementById('submitDrawBtn').addEventListener('click', async ()=>{
    if (!hasLine) { toast('Draw a line first.'); return; }
    if (map.getZoom() < REQUIRED_ZOOM) { toast(`Zoom in closer (≥${REQUIRED_ZOOM}) to submit.`); return; }

    const payload = {
      type:'new_trail',
      status:'pending',
      proposed_surface: (document.getElementById('surfaceSelect')||{}).value || 'other',
      note:(document.getElementById('drawNote')||{}).value || '',
      geometry:{ type:'LineString', coordinates: sketchCoords },
      client_meta:{ zoom: map.getZoom(), center: map.getCenter() }
    };

    try{
      const r = await fetch('/api/submissions', {
        method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)
      });
      const j = await r.json();
      toast(j.ok ? 'Submitted ✅ — open /review to approve' : ('Submit failed: ' + (j.error||'unknown')));
    }catch(e){ toast('Network error'); }
  });

  map.on('error', e => console.warn('Map error:', e && e.error));
})();
