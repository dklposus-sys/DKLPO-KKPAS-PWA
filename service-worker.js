const CACHE_NAME = 'kkpas-pwa-v5-3-1-hardened';
const APP_SHELL = ['./', './manifest.json'];
const WEB_APP_URL = 'https://script.google.com/macros/s/AKfycbzWcWIO8LZMTpEGYtK_MHj6TMXukiFji7LcaLvtYvTxIR2MML2pXyPHjWsrxZAxk0SQ/exec';
const DB_NAME = 'kkpas-offline-v1';
const DB_VERSION = 3;
const MAX_SYNC_ATTEMPTS = 5;
const RETRY_DELAYS_MS = [60*1000, 5*60*1000, 15*60*1000, 60*60*1000, 6*60*60*1000];

self.addEventListener('install', event => {
  event.waitUntil(caches.open(CACHE_NAME).then(cache => cache.addAll(APP_SHELL)).catch(() => null));
  self.skipWaiting();
});

self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys => Promise.all(keys.filter(key => key !== CACHE_NAME).map(key => caches.delete(key))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', event => {
  const request = event.request;
  if (request.method !== 'GET') return;
  const url = new URL(request.url);
  if (url.origin !== self.location.origin) return;

  event.respondWith(
    fetch(request)
      .then(response => {
        if (response && response.ok) {
          const copy = response.clone();
          caches.open(CACHE_NAME).then(cache => cache.put(request, copy));
        }
        return response;
      })
      .catch(() => caches.match(request).then(cached => cached || caches.match('./')))
  );
});

function openDb_() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VERSION);
    req.onupgradeneeded = () => {
      const db=req.result;
      if(!db.objectStoreNames.contains('drafts')) db.createObjectStore('drafts',{keyPath:'id'});
      if(!db.objectStoreNames.contains('offlineQueue')) db.createObjectStore('offlineQueue',{keyPath:'id'});
      if(!db.objectStoreNames.contains('kv')) db.createObjectStore('kv',{keyPath:'key'});
    };
    req.onsuccess=()=>resolve(req.result);
    req.onerror=()=>reject(req.error);
  });
}

async function getAllQueue_(){const db=await openDb_();return new Promise((res,rej)=>{const tx=db.transaction('offlineQueue','readonly');const r=tx.objectStore('offlineQueue').getAll();r.onsuccess=()=>{const v=r.result||[];db.close();res(v)};r.onerror=()=>{const e=r.error;db.close();rej(e)}})}
async function putQueue_(value){const db=await openDb_();return new Promise((res,rej)=>{const tx=db.transaction('offlineQueue','readwrite');tx.objectStore('offlineQueue').put(value);tx.oncomplete=()=>{db.close();res()};tx.onerror=()=>{const e=tx.error;db.close();rej(e)}})}
async function deleteQueue_(id){const db=await openDb_();return new Promise((res,rej)=>{const tx=db.transaction('offlineQueue','readwrite');tx.objectStore('offlineQueue').delete(id);tx.oncomplete=()=>{db.close();res()};tx.onerror=()=>{const e=tx.error;db.close();rej(e)}})}

function syncError_(message, retryable=true, retryAfterMs=0){
  const err=new Error(String(message||'SYNC_FAILED'));
  err.retryable=!!retryable;
  err.retryAfterMs=Number(retryAfterMs||0);
  return err;
}

async function formPost_(payload){
  const params=new URLSearchParams();
  Object.entries(payload||{}).forEach(([k,v])=>params.append(k,typeof v==='object'?JSON.stringify(v):String(v??'')));
  let response;
  try{
    response=await fetch(WEB_APP_URL,{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body:params});
  }catch(err){
    throw syncError_(err&&err.message?err.message:'NETWORK_ERROR',true);
  }
  const text=await response.text();
  let data=null;
  try{data=JSON.parse(text)}catch(_){throw syncError_('NON_JSON_RESPONSE',response.status>=500||response.status===429)}
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

async function syncOne_(record){
  if(!record||record.status==='DEAD_LETTER')return {skipped:true};
  if(record.nextAttemptAt&&new Date(record.nextAttemptAt).getTime()>Date.now())return {skipped:true};
  record.attempts=Number(record.attempts||0)+1;
  record.status='SYNCING';
  record.lastAttemptAt=new Date().toISOString();
  record.nextAttemptAt='';
  await putQueue_(record);
  const uploaded=Array.isArray(record.uploadedPhotoUrls)?record.uploadedPhotoUrls:[];
  const photos=Array.isArray(record.photos)?record.photos:[];

  for(let i=uploaded.length;i<photos.length;i++){
    const ph=photos[i];
    const uploadKey=`${record.id}-P${i+1}`;
    const r=await formPost_({action:'uploadPhoto',uploadKey,month:record.payload.month,station:record.payload.station,inspectorName:record.payload.inspectorName,base64:ph.data,comment:ph.comment||'',index:i+1,filename:ph.filename||`photo_${i+1}.jpg`});
    uploaded.push({url:r.url,fileId:r.fileId||'',resourceKey:r.resourceKey||'',comment:ph.comment||'',index:i+1,reviewAnalysis:ph.reviewAnalysis||null,reviewAccepted:ph.reviewAccepted||null,reviewedAt:ph.reviewedAt||''});
    record.uploadedPhotoUrls=uploaded;
    await putQueue_(record);
  }

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
    try{
      const result=await syncOne_(row);
      if(result&&result.ok)synced++;else skipped++;
    }catch(e){
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
      // Important: continue with the next inspection instead of blocking the whole queue.
    }
  }
  const summary={synced,failed,dead,skipped};
  // Background Sync should remain pending when retryable records still exist.
  // We processed every eligible row first, so one bad row never blocks the rest.
  if(needsRetry){const err=new Error('KKPAS_QUEUE_RETRY_PENDING');err.summary=summary;throw err;}
  return summary;
}

self.addEventListener('sync',event=>{if(event.tag==='kkpas-offline-sync')event.waitUntil(processQueue_())});
self.addEventListener('message',event=>{
  const d=event.data||{};
  if(d.type==='SYNC_KKPAS_QUEUE') event.waitUntil(processQueue_().catch(()=>null));
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
