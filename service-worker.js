const dklpoDbPool_ = new Map();
function dklpoOpenDb_(name, version, stores) {
  const key = name + ':' + version;
  const existing = dklpoDbPool_.get(key);
  if (existing) return existing.promise;
  const entry = {db:null, promise:null};
  entry.promise = new Promise((resolve, reject) => {
    let settled = false;
    const fail = error => {
      if (settled) return;
      settled = true;
      if (dklpoDbPool_.get(key) === entry) dklpoDbPool_.delete(key);
      reject(error || new Error('IndexedDB unavailable'));
    };
    let request;
    try { request = indexedDB.open(name, version); } catch (error) { fail(error); return; }
    request.onupgradeneeded = () => {
      const db = request.result;
      for (const [store, keyPath] of Object.entries(stores)) {
        if (!db.objectStoreNames.contains(store)) {
          db.createObjectStore(store, keyPath ? {keyPath} : undefined);
        }
      }
    };
    request.onblocked = () => fail(new Error('IndexedDB upgrade blocked. Tutup tab portal lama dan cuba lagi.'));
    request.onerror = () => fail(request.error);
    request.onsuccess = () => {
      const db = request.result;
      if (settled) { db.close(); return; }
      settled = true;
      entry.db = db;
      const forget = () => { if (dklpoDbPool_.get(key) === entry) dklpoDbPool_.delete(key); };
      db.onversionchange = () => { db.close(); forget(); };
      db.onclose = forget;
      resolve(db);
    };
  });
  dklpoDbPool_.set(key, entry);
  entry.promise.catch(() => { if (dklpoDbPool_.get(key) === entry) dklpoDbPool_.delete(key); });
  return entry.promise;
}
function dklpoDbRequest_(db, store, mode, method, value, key) {
  return new Promise((resolve, reject) => {
    const tx = db.transaction(store, mode);
    const target = tx.objectStore(store);
    const request = method === 'getAll' ? target.getAll()
      : method === 'put' && key !== undefined ? target.put(value, key)
      : target[method](value);
    let result;
    request.onsuccess = () => { result = request.result; };
    request.onerror = () => reject(request.error || new Error('IndexedDB request failed'));
    tx.oncomplete = () => resolve(result);
    tx.onerror = tx.onabort = () => reject(tx.error || request.error || new Error('IndexedDB transaction aborted'));
  });
}

const CACHE_PREFIX = 'kkpas-inspector-' + encodeURIComponent(self.registration.scope) + '-';
const CACHE_NAME = CACHE_PREFIX + 'r6-226';
const APP_SHELL = ['./', './index.html', './pemeriksaan-tempat-kerja-inspector.html', './manifest.json', './icons/icon-192.png', './icons/icon-512.png'];

self.addEventListener('install', event => {
  event.waitUntil(caches.open(CACHE_NAME).then(cache => Promise.all(
    APP_SHELL.map(url => cache.add(url).catch(() => null))
  )).then(() => self.skipWaiting()));
});

self.addEventListener('activate', event => {
  event.waitUntil(caches.keys().then(keys => Promise.all(
    keys.filter(key => key.startsWith(CACHE_PREFIX) && key !== CACHE_NAME).map(key => caches.delete(key))
  )).then(() => self.clients.claim()));
});

self.addEventListener('fetch', event => {
  const request = event.request;
  if (request.method !== 'GET') return;
  const url = new URL(request.url);
  if (url.origin !== self.location.origin || !url.href.startsWith(self.registration.scope)) return;
  if(request.headers.has('Authorization')||!APP_SHELL.some(path=>new URL(path,self.registration.scope).pathname===url.pathname))return;
  event.respondWith(fetch(request).then(response => {
    if (response && response.ok) {
      const copy = response.clone();
      event.waitUntil(caches.open(CACHE_NAME).then(cache => cache.put(request, copy)).catch(() => null));
    }
    return response;
  }).catch(async () => {
    const cache = await caches.open(CACHE_NAME);
    const cached = await cache.match(request);
    if (cached) return cached;
    if (request.mode === 'navigate') {
      return await cache.match(new URL('./index.html', self.registration.scope).href)
        || await cache.match(self.registration.scope) || Response.error();
    }
    return Response.error();
  }));
});

const WEB_APP_URL = 'https://dklpo-pic-api.ennedeystudio.workers.dev';  
const DB_NAME = 'kkpas-offline-v1';
const DB_VERSION = 3;
const MAX_SYNC_ATTEMPTS = 5;
const RETRY_DELAYS_MS = [60*1000, 5*60*1000, 15*60*1000, 60*60*1000, 6*60*60*1000];

function openDb_() { return dklpoOpenDb_(DB_NAME, DB_VERSION, {drafts:'id',offlineQueue:'id',kv:'key'}); }

async function getAllQueue_(value) { return await dklpoDbRequest_(await openDb_(), 'offlineQueue', 'readonly', 'getAll', value) || []; }
async function putQueue_(value) { return await dklpoDbRequest_(await openDb_(), 'offlineQueue', 'readwrite', 'put', value); }
async function deleteQueue_(value) { return await dklpoDbRequest_(await openDb_(), 'offlineQueue', 'readwrite', 'delete', value); }

function syncError_(message, retryable=true, retryAfterMs=0){
  const err=new Error(String(message||'SYNC_FAILED'));
  err.retryable=!!retryable;
  err.retryAfterMs=Number(retryAfterMs||0);
  return err;
}

async function formPost_(payload){
  const edgeSession=await dklpoBackgroundSession_();
  const params=new URLSearchParams();
  Object.entries(payload||{}).forEach(([k,v])=>params.append(k,typeof v==='object'?JSON.stringify(v):String(v??'')));
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 90000);
  let response;
  let text;
  try {
    response = await fetch(WEB_APP_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Authorization':'Bearer '+edgeSession.token },
      body: params,
      signal: controller.signal
    });
    text = await response.text();
  } catch(err) {
    throw syncError_(err && err.message ? err.message : 'NETWORK_ERROR', true);
  } finally {
    clearTimeout(timeoutId);
  }
  let data=null;
  try{data=JSON.parse(text)}catch(_){throw syncError_('NON_JSON_RESPONSE',response.status>=500||response.status===429)}
  if(response.status===401){const e=syncError_('EDGE_AUTH_REQUIRED',true);e.authRequired=true;await broadcast_({type:'DKLPO_EDGE_AUTH_REQUIRED'});throw e;}
  if(!response.ok||!data||!data.ok){
    const retryable=(data&&typeof data.retryable==='boolean')?data.retryable:(response.status===429||response.status>=500);
    const retryAfterMs=(data&&data.retryAfterMs)||0;
    throw syncError_((data&&data.error)||('HTTP_'+response.status),retryable,retryAfterMs);
  }
  return data;
}

function isPermanentSyncError_(err){
  if(err&&err.retryable===false)return true;
  const msg=String(err&&err.message?err.message:err||'');
  return /Missing required|Unsupported image|Invalid base64|Empty image|Too many photos|Photo upload incomplete|GPS_(?:OUTSIDE|INVALID|STALE|LOW_INTEGRITY)|Station already inspected|INVALID_SUBMISSION/i.test(msg);
}

function nextRetryAt_(attempts,err){
  const explicit=Number(err&&err.retryAfterMs||0);
  const fallback=RETRY_DELAYS_MS[Math.min(Math.max(0,attempts-1),RETRY_DELAYS_MS.length-1)];
  return new Date(Date.now()+Math.max(explicit,fallback)).toISOString();
}

async function broadcast_(data){
  const clients=await self.clients.matchAll({type:'window',includeUncontrolled:true});
  clients.forEach(c=>c.postMessage(data));
}


async function syncAdhocV13_(record){
  if(!record||record.status==='DEAD_LETTER')return {skipped:true};
  if(record.nextAttemptAt&&new Date(record.nextAttemptAt).getTime()>Date.now())return {skipped:true};
  if(record.status==='DIRECT_SENDING'&&Number(record.leaseUntil||0)>Date.now())return {skipped:true};
  record.leaseUntil=0;
  record.attempts=Number(record.attempts||0)+1;
  record.status='SYNCING'; record.lastAttemptAt=new Date().toISOString();record.nextAttemptAt='';
  await putQueue_(record);
  const photos=Array.isArray(record.photos)?record.photos:[];
  const map={...(record.uploadedPhotoMap||{})}, uploaded=[];
  for(let i=0;i<photos.length;i++){
    const ph=photos[i],key=record.id+'-ADHOC-'+(i+1);
    let receipt=map[key];
    if(!receipt||!receipt.fileId){
      const result=await formPost_({action:'uploadAdhocFindingPhoto',reporterName:record.payload.reporterName,
        station:record.payload.station,month:record.payload.month||['January','February','March','April','May','June','July','August','September','October','November','December'][new Date(record.createdAt||Date.now()).getMonth()],index:i+1,base64:ph.data,comment:ph.comment||'',uploadKey:key});
      if(!result.fileId)throw syncError_('ADHOC_UPLOAD_MISSING_FILE_ID',true);
      receipt={fileId:result.fileId};map[key]=receipt;record.uploadedPhotoMap=map;await putQueue_(record);
    }
    uploaded.push({fileId:receipt.fileId,comment:ph.comment||''});
  }
  const result=await formPost_({...record.payload,issueAction:record.payload.issueAction||record.payload.action||'',action:'submitAdhocFinding',photos:JSON.stringify(uploaded)});
  await deleteQueue_(record.id);
  await broadcast_({type:'KKPAS_ADHOC_SYNC_SUCCESS',station:record.payload.station,findingId:result.findingId||''});
  try{await self.registration.showNotification('Laporan NCR/CAR/PAR Berjaya',{body:'ID '+(result.findingId||'')+' • '+record.payload.station,tag:'adhoc-'+record.id,data:{url:'./'}})}catch(_){}
  return {ok:true};
}

async function syncOne_(record){
  if(record && record.type==='ADHOC_V13')return syncAdhocV13_(record);
  if(!record||record.status==='DEAD_LETTER')return {skipped:true};
  if(record.nextAttemptAt&&new Date(record.nextAttemptAt).getTime()>Date.now())return {skipped:true};
  record.attempts=Number(record.attempts||0)+1;
  record.status='SYNCING';
  record.lastAttemptAt=new Date().toISOString();
  record.nextAttemptAt='';
  await putQueue_(record);
  const photos=Array.isArray(record.photos)?record.photos:[];
  const map={...(record.uploadedPhotoMap||{})};
  const legacy=Array.isArray(record.uploadedPhotoUrls)?record.uploadedPhotoUrls:[];
  const uploaded=[];

   
  for(let i=0;i<photos.length;i++){
    const ph=photos[i];
    const stableId=String(ph&&ph.id!=null?ph.id:`legacy-${i+1}`).replace(/[^a-zA-Z0-9_-]/g,'_').slice(0,70);
    const uploadKey=`${record.id}-PHOTO-${stableId}`;
    let receipt=map[uploadKey];
    if(!receipt && !record.uploadedPhotoMap && legacy[i] && legacy[i].url) receipt=legacy[i];
    if(!receipt || !receipt.url){
      const r=await formPost_({action:'uploadPhoto',inspectorAuth:record.payload.inspectorAuth||record.payload.inspectorToken||'',teamCode:record.payload.teamCode,uploadKey,month:record.payload.month,station:record.payload.station,inspectorName:record.payload.inspectorName,base64:ph.data,comment:ph.comment||'',index:i+1,filename:ph.filename||`photo_${i+1}.jpg`});
      if(!r||!r.url) throw syncError_('UPLOAD_MISSING_URL',true);
      receipt={url:r.url,fileId:r.fileId||'',resourceKey:r.resourceKey||''};
      map[uploadKey]=receipt;
      record.uploadedPhotoMap=map;
      await putQueue_(record);  
    }
    uploaded.push({...receipt,comment:ph.comment||'',findingKey:ph.findingKey||'',index:i+1,reviewAnalysis:ph.reviewAnalysis||null,reviewAccepted:ph.reviewAccepted||null,reviewedAt:ph.reviewedAt||''});
  }
  record.uploadedPhotoUrls=uploaded;
  await putQueue_(record);

  const payload={...record.payload,action:'submitInspection',clientSubmissionId:record.payload.clientSubmissionId||record.id,offlineQueuedAt:record.createdAt||'',syncAttempt:record.attempts,checklist:JSON.stringify(record.payload.checklist||[]),photos:JSON.stringify(uploaded),findings:record.payload.findings||'[]'};
  const result=await formPost_(payload);
  await deleteQueue_(record.id);
  await broadcast_({type:'KKPAS_SYNC_SUCCESS',station:record.payload.station,id:result.id||''});
  try{await self.registration.showNotification('KKPAS Sync Berjaya',{body:`Pemeriksaan ${record.payload.station} telah dihantar.`,icon:'https://arleta.site/interactivelink/3333/DKLPO_Master_192.png',badge:'https://arleta.site/interactivelink/3333/DKLPO_Master_192.png',tag:'kkpas-sync-'+record.id,data:{url:'./'}})}catch(_){ }
  return {ok:true};
}

async function processQueue_(){
  const rows=(await getAllQueue_()).sort((a,b)=>String(a.createdAt||'').localeCompare(String(b.createdAt||'')));
  let synced=0,failed=0,dead=0,skipped=0,needsRetry=false;
  for(const row of rows){
    if(!row||row.status==='DEAD_LETTER'){dead++;continue;}
    if(row.nextAttemptAt&&new Date(row.nextAttemptAt).getTime()>Date.now()){skipped++;needsRetry=true;continue;}
    if(row.status==='DIRECT_SENDING'&&Number(row.leaseUntil||0)>Date.now()){
      skipped++;needsRetry=true;continue;
    }
    const previousAttempts=Number(row.attempts||0);
    try{
      const result=await syncOne_(row);
      if(result&&result.ok)synced++;else skipped++;
    }catch(e){
      if(e.authRequired){row.attempts=previousAttempts;row.status='AUTH_REQUIRED';row.nextAttemptAt='';row.lastError=String(e.message);await putQueue_(row);skipped++;needsRetry=true;continue;}
      failed++;
      row.lastError=String(e&&e.message?e.message:e);
      row.lastErrorAt=new Date().toISOString();
      const permanent=isPermanentSyncError_(e);
      if(permanent||Number(row.attempts||0)>=MAX_SYNC_ATTEMPTS){
        row.status='DEAD_LETTER';
        row.nextAttemptAt='';
        row.deadLetterAt=new Date().toISOString();
        await putQueue_(row);
        await broadcast_({type:'KKPAS_SYNC_DEAD_LETTER',station:row.payload&&row.payload.station,error:row.lastError,attempts:row.attempts});
        try{await self.registration.showNotification('KKPAS Sync Perlu Tindakan',{body:`${row.payload&&row.payload.station||'Pemeriksaan'} gagal dihantar selepas ${row.attempts} percubaan. Buka KKPAS untuk semakan.`,icon:'https://arleta.site/interactivelink/3333/DKLPO_Master_192.png',tag:'kkpas-dead-'+row.id,data:{url:'./'}})}catch(_){ }
        dead++;
      }else{
        row.status='QUEUED';
        row.nextAttemptAt=nextRetryAt_(Number(row.attempts||0),e);
        await putQueue_(row);
        await broadcast_({type:'KKPAS_SYNC_ERROR',station:row.payload&&row.payload.station,error:row.lastError,attempts:row.attempts,nextAttemptAt:row.nextAttemptAt});
        needsRetry=true;
      }
       
    }
  }
  const summary={synced,failed,dead,skipped};
   
   
  if(needsRetry){const err=new Error('KKPAS_QUEUE_RETRY_PENDING');err.summary=summary;throw err;}
  return summary;
}

let queueSyncPromise_=null;
function runQueueOnce_(){
  if(queueSyncPromise_)return queueSyncPromise_;
  queueSyncPromise_=processQueue_().finally(()=>{queueSyncPromise_=null});
  return queueSyncPromise_;
}
self.addEventListener('sync',event=>{if(event.tag==='kkpas-offline-sync')event.waitUntil(runQueueOnce_())});
self.addEventListener('message',event=>{
  const d=event.data||{};
  if(d.type==='SYNC_KKPAS_QUEUE') event.waitUntil(runQueueOnce_().catch(()=>null));
  if(d.type==='SHOW_NOTIFICATION'&&d.title) event.waitUntil(self.registration.showNotification(d.title,d.options||{}));
});

async function getReminder_(){
  return formPost_({action:'getNotificationCentre'});
}

async function showMonthEndReminder_(){
  const data=await getReminder_();
  const r=data&&data.reminder;
  if(!r||!r.active||!Array.isArray(r.pendingTeams)||!r.pendingTeams.length)return;
  const pendingLines=r.pendingTeams.map(p=>`${p.teamCode||p.teamName||'-'}: ${(p.stations||[]).join(', ')||p.station||'-'}`);
  await self.registration.showNotification('KKPAS Monthly Inspection',{body:pendingLines.join(' | '),icon:'https://arleta.site/interactivelink/3333/DKLPO_Master_192.png',badge:'https://arleta.site/interactivelink/3333/DKLPO_Master_192.png',tag:'kkpas-month-end-'+String(r.dateKey||''),renotify:false,data:{url:'./'}});
}

self.addEventListener('periodicsync',event=>{if(event.tag==='kkpas-month-end-reminder')event.waitUntil(showMonthEndReminder_().catch(()=>null))});
self.addEventListener('notificationclick',event=>{event.notification.close();event.waitUntil(self.clients.matchAll({type:'window',includeUncontrolled:true}).then(cs=>{for(const c of cs){if('focus'in c)return c.focus()}return self.clients.openWindow('./')}))});

async function dklpoBackgroundSession_(){
  const session = (await dklpoDbRequest_(await openDb_(), 'kv', 'readonly', 'get', 'dklpo_edge_background_v1'))?.session || null;
  if(!session?.token||Number(session.expiresAt)<=Date.now()+5000){
    const error=syncError_('EDGE_AUTH_REQUIRED: Buka portal untuk memperbaharui sesi.',true);error.authRequired=true;
    await broadcast_({type:'DKLPO_EDGE_AUTH_REQUIRED'});throw error;
  }return session;
}
self.addEventListener('message',event=>{
  if(event.data?.type!=='DKLPO_EDGE_BACKGROUND_SESSION')return;
   
  const session=event.data.session;if(!session?.token||!Number.isFinite(Number(session.expiresAt)))return;
  event.waitUntil((async()=>{await dklpoDbRequest_(await openDb_(), 'kv', 'readwrite', 'put', {key:'dklpo_edge_background_v1',session});await runQueueOnce_().catch(()=>null);})());
});
