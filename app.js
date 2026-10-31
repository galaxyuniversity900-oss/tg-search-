const SRV="Tele",TYPES=["channel","chat","bot"],USER=/^[A-Za-z0-9_]{4,32}$/,STORE="tgatlas:v1";
const CATS=["ai","dev","tech","productivity","crypto","news","finance","business","marketing","education","entertainment","gaming","music","sports","health","travel"];
const STOP=new Set("the and for with from you your this that are our all new".split(" "));
const $=id=>document.getElementById(id);
let mcp=null,dl=null,mode="tele",bridge=null,bauth=false,tab="all",shown=40,hits=new Map(),posts=[],curQ="",idx=new Map(),abort=false,running=false,seq=0,deb=null;

/* ---------- persistence (per viewer, may be unavailable) ---------- */
try{for(const r of JSON.parse(localStorage.getItem(STORE)||"[]"))idx.set(r.id,r)}catch(e){}
function save(){try{localStorage.setItem(STORE,JSON.stringify([...idx.values()].sort((a,b)=>(b.m||0)-(a.m||0)).slice(0,4000)))}catch(e){}}

/* ---------- helpers ---------- */
const fmt=n=>{n=n||0;return n>=1e6?(n/1e6).toFixed(1)+"M":n>=1e3?(n/1e3).toFixed(1)+"K":""+n};
const link=r=>USER.test(r.u||"")?"https://t.me/"+r.u:null;
function emsg(e){const c=e&&e.code;
 if(c==="needs_reauth")return"Tele needs to be reconnected in your connector settings.";
 if(c==="server_not_connected")return"Tele is not connected. Add the Tele connector in your account settings.";
 if(c==="approval_required"||c==="not_granted"||c==="consent_required")return"Please allow this page to use Tele.";
 if(c==="rate_limited")return"Rate limited. Wait a moment and try again.";
 return"Search failed"+(e&&e.message?": "+e.message:".")}
let busy=0,waiters=[];
const call=(tool,args)=>callAny(SRV,tool,args);
async function callAny(srv,tool,args){
 while(busy>=4)await new Promise(r=>waiters.push(r));
 busy++;
 try{const r=await mcp.callTool(srv,tool,args,{cache:{staleTime:60000}});return r.payload||{}}
 finally{busy--;const w=waiters.shift();if(w)w()}}
const norm=it=>({id:it.id,t:it.title||"",u:it.username||"",k:it.entity_type||"channel",m:it.subscribers_count??it.members_count??0,
 tr:it.trust_score,er:+it.er_score||0,pp:+it.posts_per_day||0,lg:it.language||[],ct:it.categories||[],g:it.growth_7d==null?null:+it.growth_7d,
 c:it.created_at||"",a:(it.description_ai_summary||"").slice(0,140),v:!!it.verified});
function add(it,asHit){if(!it||!it.id)return;const r=norm(it);idx.set(r.id,r);if(asHit)hits.set(r.id,r)}
const script=w=>/[\u0600-\u06FF]/.test(w)?"ar":/[\u0400-\u04FF]/.test(w)?"cy":/[\u4E00-\u9FFF]/.test(w)?"cjk":/[\u0900-\u097F]/.test(w)?"hi":/[a-z]/.test(w)?"la":"ot";
const words=r=>(`${r.t} ${r.a}`.toLowerCase().match(/[^\W\d_]{3,24}/gu)||[]);
function setSt(t){$("st").textContent=t}

/* ---------- real Telegram API through the local bridge (tg_atlas.py) ---------- */
const ready=()=>mode==="bridge"?!!(bridge&&bauth):!!mcp;
async function bcall(path,body){
 try{const r=await fetch(bridge.base+path,{method:body?"POST":"GET",headers:{"X-Token":bridge.token,"Content-Type":"application/json"},body:body?JSON.stringify(body):undefined});
  const x=await r.text();try{return JSON.parse(x)}catch(e){return{error:r.status===403?"The link token is wrong. Paste the newest link printed by tg_atlas.py.":"Unexpected reply from the server."}}}
 catch(e){return{error:"Cannot reach tg_atlas.py. Is it running on this device?"}}}
function brShow(st){const c=$("conn"),cf=!!(st&&st.configured),au=!!(st&&st.authorized),sg=st&&st.stage;
 $("brcfg").hidden=cf;$("brph").hidden=!(cf&&!au&&sg==="phone");$("brcode").hidden=!(cf&&!au&&sg==="code");$("brpw").hidden=!(cf&&!au&&sg==="password");
 bauth=au;
 if(au){c.textContent="Telegram API: "+(st.me||"signed in");c.className="pill ok";$("brd").open=false;$("brmsg").textContent="Connected to the real Telegram API."}
 else{c.textContent="Telegram API: sign in";c.className="pill bad"}}
async function brConnect(){let u;
 try{u=new URL($("brurl").value.trim())}catch(e){$("brmsg").textContent="Paste the full link printed by tg_atlas.py.";return}
 if(!["127.0.0.1","localhost"].includes(u.hostname)){$("brmsg").textContent="Only a bridge running on this device (127.0.0.1) is allowed.";return}
 bridge={base:u.origin,token:u.searchParams.get("t")||""};
 const st=await bcall("/api/status");if(st.error){$("brmsg").textContent=st.error;bridge=null;return}
 try{localStorage.setItem("tgatlas:bridge",$("brurl").value.trim())}catch(e){}
 brShow(st);setSt(bauth?"Connected to the Telegram API.":"Finish signing in to Telegram above.")}
async function brStep(path,body,ok){const r=await bcall(path,body);$("brmsg").textContent=r.error||ok||"";brShow(await bcall("/api/status"))}
$("brgo").onclick=brConnect;
$("brsave").onclick=()=>brStep("/api/config",{api_id:$("braid").value,api_hash:$("brhash").value},"Keys saved.");
$("brsend").onclick=()=>brStep("/api/send_code",{phone:$("brphone").value},"Code sent. Check your Telegram app.");
$("brsign").onclick=()=>brStep("/api/sign_in",{code:$("brcd").value});
$("brpwgo").onclick=()=>brStep("/api/password",{password:$("brpass").value});

/* ---------- search ---------- */
async function searchOne(q,types){
 if(mode==="bridge"){const p=await bcall("/api/tg/search",{query:q,types:types});
  if(p.error)return{n:0,more:false,err:{message:p.error},own:[]};
  const items=p.items||[],own=[];for(const it of items){add(it,q===curQ||curQ&&q.startsWith(curQ));own.push(norm(it))}
  return{n:items.length,more:!!p.more,err:null,own}}
 const rs=await Promise.allSettled(types.map(t=>call("search_telegram",{query:q,type:t,limit:100})));
 let n=0,more=false,err=null;const own=[];
 for(const r of rs){if(r.status==="fulfilled"){const items=r.value.items||[];for(const it of items){add(it,q===curQ||curQ&&q.startsWith(curQ));own.push(norm(it));n++}
   if((r.value.total||0)>items.length)more=true}else err=r.reason}
 return{n,more,err,own}}
async function live(){
 const q=$("q").value.trim(),my=++seq;curQ=q;shown=40;
 if(!q){hits=new Map();posts=[];render();setSt(idx.size?idx.size+" saved in your index":"Type to search Telegram.");
if("serviceWorker" in navigator && location.protocol.startsWith("http")) navigator.serviceWorker.register("sw.js").catch(()=>{});return}
 if(!ready()){setSt(mode==="bridge"?"Connect to the Telegram API first (top of the page).":"Tele is not available here.");return}hits=new Map();
 setSt("Searching…");
 try{
  if(tab==="post"){const p=mode==="bridge"?await bcall("/api/tg/posts",{query:q,limit:50}):await call("search_posts",{query:q,limit:50});if(p.error)throw{message:p.error};if(my!==seq)return;posts=p.items||[];setSt((p.total||posts.length)+" matching posts")}
  else{const r=await searchOne(q,tab==="all"?TYPES:[tab]);if(my!==seq)return;
   if(!hits.size&&r.err)return setSt(emsg(r.err));
   save();setSt(hits.size+" results"+(r.more?" · more exist: use Deep search":""))}
 }catch(e){if(my===seq)setSt(emsg(e))}
 if(my===seq)render()}
async function deep(){
 const root=$("q").value.trim();if(!root||!ready())return;curQ=root;hits=new Map();abort=false;lock(true);
 const seen=new Set(),queue=[[root,0]];let calls=0;
 while(queue.length&&!abort&&calls<60){
  const batch=queue.splice(0,3);
  await Promise.all(batch.map(async([s,d])=>{if(seen.has(s))return;seen.add(s);calls++;
   const r=await searchOne(s,TYPES);
   if(r.more&&d<2){const c=new Map();for(const x of r.own)for(const w of new Set(words(x)))c.set(w,(c.get(w)||0)+1);
    const have=new Set(s.toLowerCase().split(/\s+/));
    [...c].filter(([w])=>!have.has(w)&&!STOP.has(w)).sort((a,b)=>b[1]-a[1]).slice(0,10).forEach(([w])=>queue.push([s+" "+w,d+1]))}}));
  setSt(`Deep search: ${calls} queries · ${hits.size} results`);render()}
 save();lock(false);setSt(`Deep search done: ${hits.size} results from ${calls} queries.`);render()}
async function discover(){
 if(mode==="bridge"){setSt("Category lists exist only in Tele. In Telegram API mode use Deep search or Auto discovery.");return}
 if(!mcp)return;abort=false;lock(true);hits=new Map();curQ="";let n=0;
 await Promise.all(CATS.map(async c=>{if(abort)return;try{const p=await call("best_channels_by_category",{category:c,limit:50});
  for(const it of p.items||[]){add(it,true);n++}}catch(e){}setSt(`Loading categories… ${hits.size} channels`);render()}));
 save();lock(false);$("sort").value="c";setSt(`Loaded ${hits.size} top channels. "Newest in catalog" is when Tele first listed them, not their Telegram creation date.`);render()}
function mine(tried,k){
 const c=new Map();for(const r of idx.values())for(const w of new Set(words(r)))c.set(w,(c.get(w)||0)+1);
 const b={};for(const [w] of [...c].sort((a,b)=>b[1]-a[1]).slice(0,4000)){if(STOP.has(w)||tried.has(w))continue;(b[script(w)]=b[script(w)]||[]).push(w)}
 const out=[];while(out.length<k&&Object.values(b).some(a=>a.length))for(const a of Object.values(b))if(a.length&&out.length<k)out.push(a.shift());
 return out}
async function auto(){
 if(!ready())return;abort=false;lock(true);hits=new Map();curQ="";
 const end=Date.now()+(+$("mins").value||10)*60000,tried=new Set(),q=[];let n=0;
 const s=$("q").value.trim();if(s)q.push(s);
 if(idx.size<20)await discover(),abort=false,lock(true);
 while(!abort&&Date.now()<end){
  if(q.length<6)q.push(...mine(tried,12));
  if(!q.length){setSt("Nothing new to explore. Try a starting query.");break}
  const batch=q.splice(0,4);batch.forEach(w=>tried.add(w));
  await Promise.all(batch.map(w=>searchOne(w,TYPES)));n+=batch.length;save();
  setSt(`Auto discovery: ${n} keywords · ${idx.size} in your index · last: ${batch[0]}`);render()}
 lock(false);if(abort)setSt(`Stopped. ${idx.size} entries in your index.`)}
function lock(on){running=on;$("autobtn").disabled=on;$("deepbtn").disabled=on;$("discbtn").disabled=on;$("stopbtn").disabled=!on}

/* ---------- render ---------- */
function list(){
 const src=curQ||hits.size?[...hits.values()]:[...idx.values()];
 const kinds={channel:"channel",chat:"chat",bot:"bot"},ml=+$("minm").value||0,lg=$("lang").value,ct=$("cat").value,vo=$("ver").checked;
 let a=src.filter(r=>(tab==="all"||tab==="post"||r.k===kinds[tab])&&r.m>=ml&&(!lg||r.lg.includes(lg))&&(!ct||r.ct.includes(ct))&&(!vo||r.v));
 const s=$("sort").value;
 if(s!=="rel")a=a.slice().sort((x,y)=>s==="c"?(y.c||"").localeCompare(x.c||""):s==="g"?(y.g??-1e18)-(x.g??-1e18):s==="m"?y.m-x.m:(y[s]||0)-(x[s]||0));
 return a}
function fill(id,vals,label){const el=$(id),cur=el.value;el.textContent="";const o=document.createElement("option");o.value="";o.textContent=label;el.append(o);
 for(const v of vals){const p=document.createElement("option");p.value=v;p.textContent=v;el.append(p)}el.value=vals.includes(cur)?cur:""}
function card(r){const c=document.createElement("div");c.className="card";const l=document.createElement("a"),u=link(r);
 if(u){l.href=u;l.target="_blank";l.rel="noopener"}l.textContent=r.t||"@"+r.u;l.dir="auto";
 const m=document.createElement("div");m.className="m";
 m.textContent=[r.k,fmt(r.m)+(r.k==="chat"?" members":" subscribers"),r.u?"@"+r.u:"",r.v?"verified":"",r.lg.join("/"),r.ct.join(", ")].filter(Boolean).join(" · ");
 const x=document.createElement("div");x.className="m";
 x.textContent=[r.er?"engagement "+r.er:"",r.pp?r.pp+" posts/day":"",r.g!=null?"7d growth "+r.g:"",r.tr!=null?"trust "+r.tr:""].filter(Boolean).join(" · ");
 c.append(l,m);if(x.textContent)c.append(x);
 if(r.a){const d=document.createElement("div");d.className="d";d.dir="auto";d.textContent=r.a;c.append(d)}return c}
function pcard(p){const c=document.createElement("div");c.className="card";const t=document.createElement("div");t.className="d";t.dir="auto";t.textContent=p.snippet||"";
 const m=document.createElement("div");m.className="m";m.textContent=[p.channel_username,p.posted_at?new Date(p.posted_at*1000).toLocaleDateString():"",p.views?fmt(p.views)+" views":"",p.is_market?"marketplace":""].filter(Boolean).join(" · ");
 c.append(t,m);if(/^https:\/\/t\.me\//.test(p.tme_url||"")){const l=document.createElement("a");l.href=p.tme_url;l.target="_blank";l.rel="noopener";l.textContent="Open post";c.append(l)}return c}
function render(){
 const o=$("out");o.textContent="";$("mktw").hidden=tab!=="post";$("exprow").hidden=tab==="post";
 if(tab==="post"){const a=posts.filter(p=>!($("mkt").checked&&p.is_market));$("more").hidden=a.length<=shown;
  if(!a.length){o.innerHTML="";const d=document.createElement("div");d.className="msg";d.textContent=curQ?"No posts found.":"Type a query to search posts.";o.append(d)}
  a.slice(0,shown).forEach(p=>o.append(pcard(p)));return}
 const a=list();
 const lgs=new Set(),cts=new Set();for(const r of (curQ||hits.size?[...hits.values()]:[...idx.values()])){r.lg.forEach(x=>lgs.add(x));r.ct.forEach(x=>cts.add(x))}
 fill("lang",[...lgs].sort(),"Any language");fill("cat",[...cts].sort(),"Any category");
 if(!a.length){const d=document.createElement("div");d.className="msg";d.textContent=curQ?"No results yet.":"Your index is empty. Search, or run auto discovery.";o.append(d)}
 a.slice(0,shown).forEach(r=>o.append(card(r)));$("more").hidden=a.length<=shown}

/* ---------- export ---------- */
async function saveFile(name,text){if(!dl){try{const a=document.createElement("a");a.href=URL.createObjectURL(new Blob([text],{type:"text/plain;charset=utf-8"}));a.download=name;document.body.append(a);a.click();a.remove();setSt("Saved "+name)}catch(e){setSt("Saving failed.")}return}
 try{await dl.save({filename:name,data:text});setSt("Saved "+name)}catch(e){setSt("Save was cancelled.")}}
const rows=()=>list().filter(r=>link(r));
$("savetxt").onclick=()=>saveFile("telegram-links.txt",rows().map(r=>`${r.t||r.u} | ${link(r)} | ${fmt(r.m)}`).join("\n"));
$("savecsv").onclick=()=>{const q=s=>'"'+String(s??"").replace(/"/g,'""')+'"';
 saveFile("telegram-links.csv","\uFEFF"+["title,link,kind,members,language,categories,engagement,posts_per_day,growth_7d,catalog_date"].concat(
  rows().map(r=>[r.t,link(r),r.k,r.m,r.lg.join("/"),r.ct.join("/"),r.er,r.pp,r.g,r.c.slice(0,10)].map(q).join(","))).join("\n"))};
$("clr").onclick=()=>{if(confirm("Remove the saved index from this device?")){idx=new Map();hits=new Map();try{localStorage.removeItem(STORE)}catch(e){}render();setSt("Index cleared.")}};

/* ---------- connector hub: any number of declared MCP connectors ---------- */
let hubS=[],hubSrv=null,hubRes={};
const rdOnly=t=>(t.annotations&&t.annotations.readOnlyHint===true)||/^(search|list|get|find|read|query|fetch|browse)/i.test(t.name);
async function hubLoad(){
 try{hubS=((await mcp.listTools()).servers||[]).filter(s=>s.kind!=="artifact")}catch(e){$("hubmsg").textContent=emsg(e);return}
 const te=hubS.find(s=>s.server===SRV),pc=$("conn");
 if(!te){pc.textContent="Tele not connected";pc.className="pill bad"}
 else if(te.authStatus==="needs_reauth"){pc.textContent="Tele needs reconnecting";pc.className="pill bad"}
 else{pc.textContent="Tele connected";pc.className="pill ok"}
 hubFilter()}
function hubFilter(){
 const f=$("hubq").value.trim().toLowerCase(),box=$("hublist");box.textContent="";
 const list=hubS.filter(s=>!f||s.server.toLowerCase().includes(f));
 $("hubmsg").textContent=hubS.length+" connector(s) on this page · "+list.length+" shown";
 for(const s of list){const l=document.createElement("label");l.className="m";
  const c=document.createElement("input");c.type="checkbox";c.value=s.server;c.checked=true;
  const t=document.createElement("span");t.textContent=s.server+" ("+s.tools.length+" tools"+(s.authStatus==="needs_reauth"?", reconnect needed":"")+")";
  const b=document.createElement("button");b.className="chip";b.textContent="Tools";b.onclick=()=>hubTools(s);
  l.append(c,t,b);box.append(l)}}
function hubTools(s){hubSrv=s;const sel=$("hubtool");sel.textContent="";
 for(const t of s.tools){const o=document.createElement("option");o.value=t.name;o.textContent=t.name+(rdOnly(t)?"":" (may change data)");sel.append(o)}
 hubForm()}
async function hubForm(){
 const f=$("hubform");f.textContent="";if(!hubSrv||!$("hubtool").value)return;
 let d;try{d=await mcp.describeTool(hubSrv.server,$("hubtool").value)}catch(e){f.textContent=emsg(e);return}
 const sc=d.inputSchema||{},props=sc.properties||{},req=sc.required||[];
 const ds=document.createElement("div");ds.className="m";ds.textContent=(d.description||"").slice(0,300);f.append(ds);
 for(const [k,v] of Object.entries(props)){const l=document.createElement("label");l.className="m";l.textContent=k+(req.includes(k)?" *":"")+" ("+(v.type||"json")+") ";
  let i;if(Array.isArray(v.enum)){i=document.createElement("select");for(const e of v.enum){const o=document.createElement("option");o.value=e;o.textContent=e;i.append(o)}}
  else if(v.type==="boolean"){i=document.createElement("input");i.type="checkbox"}
  else{i=document.createElement("input");i.type="text";i.placeholder=(v.description||"").slice(0,60)}
  i.dataset.k=k;i.dataset.t=v.type||"json";l.append(i);f.append(l)}}
function hubArgs(){const a={};
 for(const i of $("hubform").querySelectorAll("[data-k]")){
  if(i.type==="checkbox"){if(i.checked)a[i.dataset.k]=true;continue}
  if(i.value==="")continue;let v=i.value;const t=i.dataset.t;
  if(t==="number"||t==="integer")v=Number(v);else if(t!=="string"&&i.tagName!=="SELECT"){try{v=JSON.parse(v)}catch(e){}}
  a[i.dataset.k]=v}
 return a}
function hubShow(el,title,data){
 el.textContent="";const h=document.createElement("div");h.className="m";
 const arr=Array.isArray(data)?data:(data&&(data.items||data.results));
 h.textContent=title+(Array.isArray(arr)?" · "+arr.length+" items":"");el.append(h);
 const txt=typeof data==="string"?data:JSON.stringify(data,null,1)||"",seen=new Set();
 for(const u of (txt.match(/https?:\/\/[^\s"'<>\\]+/g)||[])){if(seen.size>=30)break;if(seen.has(u))continue;seen.add(u);
  const a=document.createElement("a");a.href=u;a.target="_blank";a.rel="noopener noreferrer";a.textContent=u.slice(0,80);a.style.display="block";el.append(a)}
 const pre=document.createElement("pre");pre.textContent=txt.slice(0,6000)+(txt.length>6000?"\n… (truncated)":"");el.append(pre)}
async function hubRun(){
 if(!hubSrv)return $("hubout").textContent="Pick a connector with its Tools button first.";
 const t=hubSrv.tools.find(x=>x.name===$("hubtool").value);if(!t)return;
 if(!rdOnly(t)&&!confirm("This tool may change data in "+hubSrv.server+". Run it?"))return;
 $("hubout").textContent="Running…";
 try{const r=await mcp.callTool(hubSrv.server,t.name,hubArgs());hubRes[hubSrv.server]=r.payload??r;hubShow($("hubout"),hubSrv.server+" · "+t.name,hubRes[hubSrv.server])}
 catch(e){$("hubout").textContent=hubSrv.server+": "+emsg(e)}}
async function hubFed(){
 const q=$("hubfq").value.trim(),pat=($("hubfn").value.trim()||"search").toLowerCase();if(!q||!mcp)return;
 const names=[...$("hublist").querySelectorAll("input:checked")].map(i=>i.value),out=$("hubout");out.textContent="";hubRes={};let done=0;
 await Promise.all(names.map(async name=>{
  const sec=document.createElement("div");sec.className="card";sec.textContent=name+": …";out.append(sec);
  const s=hubS.find(x=>x.server===name),t=s&&s.tools.find(x=>x.name.toLowerCase().includes(pat)&&rdOnly(x));
  try{
   if(!t)sec.textContent=name+": no read-only tool matching “"+pat+"”";
   else{const d=await mcp.describeTool(name,t.name),props=(d.inputSchema&&d.inputSchema.properties)||{};
    const key=["query","q","search","text","keywords","term"].find(k=>props[k])||Object.keys(props).find(k=>props[k].type==="string");
    if(!key)sec.textContent=name+": "+t.name+" has no text input";
    else{hubRes[name]=await callAny(name,t.name,{[key]:q});hubShow(sec,name+" · "+t.name,hubRes[name])}}
  }catch(e){sec.textContent=name+": "+emsg(e)}
  $("hubmsg").textContent="Search all: "+(++done)+"/"+names.length+" connectors answered"}))}
$("hubq").oninput=hubFilter;$("hubreload").onclick=()=>mcp&&hubLoad();$("hubtool").onchange=hubForm;$("hubrun").onclick=hubRun;$("hubfed").onclick=hubFed;
$("hubfq").onkeydown=e=>{if(e.key==="Enter")hubFed()};
$("hubsave").onclick=()=>saveFile("connector-results.json",JSON.stringify(hubRes,null,1));

/* ---------- wiring ---------- */
for(const [k,v] of [["all","All"],["channel","Channels"],["chat","Groups"],["bot","Bots"],["post","Posts"]]){
 const b=document.createElement("button");b.className="chip"+(k===tab?" on":"");b.textContent=v;
 b.onclick=()=>{tab=k;shown=40;[...$("tabs").children].forEach(c=>c.classList.toggle("on",c===b));live()};$("tabs").append(b)}
$("q").oninput=()=>{clearTimeout(deb);deb=setTimeout(live,400)};
$("q").onkeydown=e=>{if(e.key==="Enter"){clearTimeout(deb);live()}};
const FX=JSON.parse($("fx").textContent);for(const n of Object.keys(FX)){const b=document.createElement("button");b.textContent="Save "+n;b.onclick=()=>saveFile(n,FX[n]);$("fbtn").append(b)}
$("go").onclick=live;$("deepbtn").onclick=deep;$("discbtn").onclick=discover;$("autobtn").onclick=auto;
$("stopbtn").onclick=()=>{abort=true};$("more").onclick=()=>{shown+=60;render()};
["sort","lang","cat","ver","mkt"].forEach(i=>$(i).onchange=()=>{shown=40;render()});$("minm").oninput=()=>{shown=40;render()};
render();setSt(idx.size?idx.size+" saved in your index":"Type to search Telegram.");
(async()=>{
 try{mcp=await claude.use("mcp")}catch(e){mcp=null}
 try{dl=await claude.use("downloads")}catch(e){dl=null}
 if(!dl)$("hubsave").hidden=true;
 if(mcp)hubLoad();else $("hubmsg").textContent="Connectors are unavailable in this view."
 const c=$("conn");if(mcp){mode="tele";$("brd").hidden=true;c.textContent="Tele connected";c.className="pill ok"}else{mode="bridge";$("hub").hidden=true;c.textContent="Telegram API: not connected";c.className="pill bad";$("brd").open=true;try{const sv=localStorage.getItem("tgatlas:bridge");if(sv){$("brurl").value=sv;brConnect()}}catch(e){}
  setSt("Run tg_atlas.py, paste its link above, and sign in to search Telegram directly.")}
})();
