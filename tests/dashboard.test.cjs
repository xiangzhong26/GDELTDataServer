// Run with: node tests/dashboard.test.cjs (no third-party dependencies).
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const html = fs.readFileSync(path.join(__dirname, '../gdelt_server/dashboard.html'), 'utf8');
const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];
const elements = new Map();
function element(id) {
  if (!elements.has(id)) elements.set(id, {
    value: id === 'days' ? '30' : id === 'view' ? 'overview' : '',
    style: {}, classList: {add() {}, remove() {}, toggle() {}},
    children: [], replaceChildren() {this.children = []},
    append(child) {this.children.push(child)}, setAttribute() {},
  });
  return elements.get(id);
}
let interval;
const context = vm.createContext({
  document: {getElementById: element, createElementNS: (_, tag) => ({tag, setAttribute() {}})},
  fetch: () => new Promise(() => {}), AbortController, Date, console,
  setTimeout: () => 1, clearTimeout() {}, setInterval: fn => {interval = fn},
});
vm.runInContext(script, context);
assert.equal(typeof interval, 'function', 'Empty dashboard must still install polling');
vm.runInContext(`
  state.polling=false;
  var loads=0, parameterCalls=0, failParameters=true;
  var server={monitor_enabled:false,worker_alive:true,busy:false,storage:{ledger:{},latest:{},errors:[],database_bytes:0,free_disk_bytes:1},snapshot:null,parameter_version:0,data_version:0};
  api=async path=>{
    if(path==='/api/gdelt/status')return server;
    parameterCalls++;
    if(failParameters){failParameters=false;throw Error('temporary network error')}
    return {params:{}};
  };
  load=async()=>{loads++;state.version=server.data_version;state.lastLoad=Date.now()};
`, context);
(async () => {
  await interval(); // First parameter request fails; the next poll must retry it.
  await interval();
  assert.equal(vm.runInContext('parameterCalls', context), 2);
  assert.equal(vm.runInContext('loads', context), 1, 'Empty results should load');
  vm.runInContext('server.data_version=1', context);
  await interval();
  assert.equal(vm.runInContext('loads', context), 2, 'New background data should refresh an empty page');
  vm.runInContext(`server.concurrency={download_workers:16,parser_workers:4};server.last_run={processed:24,seconds:4.95,files_per_second:4.85,scheduling_seconds:.005,publishing_seconds:0,stage_seconds:{download:44,parse_compute:4,parse_queue_transfer:2,commit_wait:1,commit:1}}`, context);
  await interval();
  assert.match(element('performance').textContent, /16 个文件任务 \/ 4 个解析进程/);
  assert.match(element('performance').textContent, /解析排队\/传输/);
  element('downloadWorkers').value='32';element('parserWorkers').value='12';
  element('downloadWorkers').oninput();
  await interval();
  assert.equal(element('downloadWorkers').value, '32', 'Polling must preserve unsaved edits');
  vm.runInContext(`var controlCalls=[],oldApi=api;api=async(path,method,body)=>{if(path==='/api/admin/concurrency'){controlCalls.push({path,method,body});server.pending_concurrency=body;return {accepted:true,reason:'saved'}}if(path==='/api/admin/pause'){controlCalls.push({path,method});return {accepted:true}}return oldApi(path,method,body)}`, context);
  await element('saveConcurrency').onclick();
  assert.equal(vm.runInContext('controlCalls[0].method', context), 'PUT');
  assert.equal(vm.runInContext('controlCalls[0].body.parser_workers', context), 12);
  assert.match(element('concurrencyState').textContent, /等待在途文件收尾/);
  assert.equal(element('saveConcurrency').disabled, false);
  vm.runInContext('server.concurrency=server.pending_concurrency;server.pending_concurrency=null', context);
  await interval();
  assert.match(element('concurrencyState').textContent, /32 个文件任务 \/ 12 个解析进程/);
  await element('pauseAll').onclick();
  assert.equal(vm.runInContext('controlCalls[1].path', context), '/api/admin/pause');
  element('downloadWorkers').value='65';
  await element('saveConcurrency').onclick();
  assert.equal(vm.runInContext('controlCalls.length', context), 2, 'Invalid limits must not be submitted');
  element('view').onchange();
  element('days').onchange();
  element('refresh').onclick();
  assert.equal(vm.runInContext('loads', context), 5, 'Filter and refresh controls must load data');
  vm.runInContext(`trend([{timestamp:1,event_count:2,complete:true},{timestamp:2,event_count:null,complete:false},{timestamp:3,event_count:0,complete:true}], 'event_count')`, context);
  assert.equal(element('trend').children.filter(c => c.tag === 'polyline').length, 2,
    'A missing collection must split the chart; a collected zero remains a point');
  console.log('Dashboard polling, retry, controls and missing-data chart checks passed.');
})().catch(error => {console.error(error); process.exitCode = 1});
