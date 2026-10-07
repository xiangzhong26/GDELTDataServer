const {test}=require('node:test'),assert=require('node:assert/strict');
const fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
const html=fs.readFileSync(path.join(__dirname,'../gdelt_server/dashboard.html'),'utf8');
const script=html.match(/<script>([\s\S]*?)<\/script>/)[1].replace('restoreConnection();status();setInterval(status,3000);','');
const KEY='gdelt.dashboard.admin-token.v1';
function page(storage=new Map(),statusCode=200,blocked=false){
 const nodes=new Map(),calls=[];
 function element(id){if(!nodes.has(id))nodes.set(id,{value:'',checked:false,disabled:false,style:{},classList:{add(){},remove(){},toggle(){}},setAttribute(){},replaceChildren(){},append(){}});return nodes.get(id);}
 const c=vm.createContext({document:{getElementById:element},localStorage:{getItem(k){if(blocked)throw Error('blocked');return storage.get(k)||null;},setItem(k,v){if(blocked)throw Error('blocked');storage.set(k,v);},removeItem(k){if(blocked)throw Error('blocked');storage.delete(k);}},
 fetch:async(url,options)=>{calls.push({url,options});return {ok:statusCode===200,status:statusCode,text:async()=>JSON.stringify(statusCode===200?{}:{detail:'访问令牌无效'})};},
 AbortController,Date,console,setTimeout:()=>1,clearTimeout(){},setInterval(){}});
 vm.runInContext(script,c);vm.runInContext('status=async()=>{}',c);
 return {run:s=>vm.runInContext(s,c),element,storage,calls};
}
test('explicit remember validates once and restores across page instances',async()=>{
 const p=page();p.element('token').value='test-management-token';p.element('rememberToken').checked=true;
 await p.run('connectWorkspace()');assert.equal(p.storage.get(KEY),'test-management-token');
 assert.equal(p.calls.length,1);assert.equal(p.calls[0].options.headers.Authorization,'Bearer test-management-token');
 assert.equal(p.element('token').value,'');
 const again=page(p.storage);again.run('restoreConnection()');
 assert.equal(again.run('state.token'),'test-management-token');assert.equal(again.element('rememberToken').checked,true);
 await again.run("api('/api/gdelt/status')");assert.equal(again.calls[0].options.headers.Authorization,'Bearer test-management-token');
});
test('unchecked remember never persists, and unchecking removes saved credentials',async()=>{
 const p=page();p.element('token').value='test-token';await p.run('connectWorkspace()');assert.equal(p.storage.size,0);
 p.element('rememberToken').checked=true;p.element('rememberToken').onchange();assert.equal(p.storage.get(KEY),'test-token');
 p.element('rememberToken').checked=false;p.element('rememberToken').onchange();assert.equal(p.storage.size,0);assert.equal(p.run('state.token'),'test-token');
});
test('forget clears browser state and never sends a pause or other control request',async()=>{
 const p=page(new Map([[KEY,'saved-token']]));p.run('restoreConnection();forgetConnection()');
 assert.equal(p.storage.size,0);assert.equal(p.run('state.token'),'');assert.equal(p.run('state.disconnected'),true);
 await assert.rejects(p.run("api('/api/admin/backfill/resume','POST')"),/请先/);assert.equal(p.calls.length,0);
});
test('invalid saved token is discarded instead of repeatedly polling unauthorized',async()=>{
 const p=page(new Map([[KEY,'invalid-token']]),401);p.run('restoreConnection()');
 await assert.rejects(p.run("api('/api/gdelt/status')"),/访问令牌无效/);
 assert.equal(p.storage.size,0);assert.equal(p.run('state.disconnected'),true);
 await assert.rejects(p.run("api('/api/gdelt/status')"),/请先/);assert.equal(p.calls.length,1);
});
test('blocked browser storage still permits a connection for the current page',async()=>{
 const p=page(new Map(),200,true);p.run('restoreConnection()');p.element('token').value='test-token';p.element('rememberToken').checked=true;
 await p.run('connectWorkspace()');assert.equal(p.run('state.authenticated'),true);assert.equal(p.run('state.token'),'test-token');
 assert.match(p.element('message').textContent,/浏览器阻止保存/);
});
