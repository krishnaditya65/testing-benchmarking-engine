# TestForge — Test Scripts (reference, NOT executed)

Assumptions to confirm: pipeline YAML schema (use `tools/testforge-cli/examples/pipelines/checkout-load-test.yaml` as the template), GraphQL operation names (`docs/api-specifications/pipeline-api-gateway.graphql`), TQ REST paths under `/telemetry`, worker flags, ClickHouse table `telemetry_events`, CLI binary `testforge`. Needs go, bash, curl, jq, docker, python3, kind/kubectl/helm (chaos + IaC), cargo, pnpm.

## calibrated_target.go — ground-truth target (the oracle for all accuracy tests)
```go
// go run calibrated_target.go -addr :9900   | control: /__cfg?fixed_ms=50&slow_pct=5&slow_ms=500&err_pct=10&reset_pct=1&stall_every_s=10&stall_ms=2000   | truth: /__truth , /__reset
package main
import ("encoding/json";"flag";"math/rand";"net/http";"strconv";"sync";"sync/atomic";"time")
type cfg struct{ FixedMs, SlowPct, SlowMs, ErrPct, ResetPct, StallEveryS, StallMs int }
var c atomic.Value; var mu sync.Mutex; var perSec = map[int64]int64{}; var total, errs, resets int64; var start = time.Now(); var srcs sync.Map
func main(){ addr:=flag.String("addr",":9900",""); flag.Parse(); c.Store(cfg{})
  http.HandleFunc("/__cfg",func(w http.ResponseWriter,r *http.Request){ q:=r.URL.Query(); g:=func(k string)int{v,_:=strconv.Atoi(q.Get(k));return v}
    c.Store(cfg{g("fixed_ms"),g("slow_pct"),g("slow_ms"),g("err_pct"),g("reset_pct"),g("stall_every_s"),g("stall_ms")}); w.Write([]byte("ok")) })
  http.HandleFunc("/__reset",func(w http.ResponseWriter,r *http.Request){ mu.Lock(); perSec=map[int64]int64{}; mu.Unlock(); atomic.StoreInt64(&total,0); atomic.StoreInt64(&errs,0); atomic.StoreInt64(&resets,0); start=time.Now(); srcs=sync.Map{} })
  http.HandleFunc("/__truth",func(w http.ResponseWriter,r *http.Request){ mu.Lock(); defer mu.Unlock(); var ips []string; srcs.Range(func(k,_ any)bool{ips=append(ips,k.(string));return true})
    json.NewEncoder(w).Encode(map[string]any{"total":atomic.LoadInt64(&total),"errors":atomic.LoadInt64(&errs),"resets":atomic.LoadInt64(&resets),"perSecond":perSec,"sources":ips,"lastRequestAgoMs":time.Since(time.Unix(0,atomic.LoadInt64(&last))).Milliseconds()}) })
  http.HandleFunc("/",func(w http.ResponseWriter,r *http.Request){ now:=time.Now(); atomic.StoreInt64(&last,now.UnixNano()); atomic.AddInt64(&total,1); mu.Lock(); perSec[now.Unix()]++; mu.Unlock(); srcs.Store(r.RemoteAddr[:len(r.RemoteAddr)-6],1)
    k:=c.Load().(cfg); if k.StallEveryS>0 && int(now.Sub(start).Seconds())%k.StallEveryS==0 { time.Sleep(time.Duration(k.StallMs)*time.Millisecond) }
    d:=k.FixedMs; p:=rand.Intn(100); if p<k.SlowPct { d=k.SlowMs }; time.Sleep(time.Duration(d)*time.Millisecond)
    if rand.Intn(100)<k.ResetPct { atomic.AddInt64(&resets,1); if hj,ok:=w.(http.Hijacker);ok{ cn,_,_:=hj.Hijack(); cn.Close(); return } }
    if rand.Intn(100)<k.ErrPct { atomic.AddInt64(&errs,1); w.WriteHeader(503); return }; w.Write([]byte(`{"ok":true}`)) })
  http.ListenAndServe(*addr,nil) }
var last int64
```

## lib.sh
```bash
#!/usr/bin/env bash
set -euo pipefail
GW=${GW:-http://localhost:4000/graphql}; TQ=${TQ:-http://localhost:4500}; CAL=${CAL:-http://localhost:9900}; CLI=${CLI:-testforge}; JWT=${JWT:-}
PASS=0; FAIL=0; ok(){ PASS=$((PASS+1)); echo "PASS $1"; }; ko(){ FAIL=$((FAIL+1)); echo "FAIL $1 :: ${2:-}"; }
gql(){ curl -s $GW -H 'Content-Type: application/json' ${JWT:+-H "Authorization: Bearer $JWT"} -d "$(jq -nc --arg q "$1" --argjson v "${2:-{\}}" '{query:$q,variables:$v}')"; }
calcfg(){ curl -s "$CAL/__cfg?$1" >/dev/null; curl -s $CAL/__reset >/dev/null; }; truth(){ curl -s $CAL/__truth; }
pipeline(){ # rps duration_s [extra yaml] -> writes /tmp/p.yaml   (schema: VERIFY against examples/pipelines/checkout-load-test.yaml)
cat > /tmp/p.yaml <<EOF
name: qa-$RANDOM
stages:
  - id: load
    type: http-load
    target: $CAL/checkout
    rate: { model: open, rps: $1 }
    duration: ${2}s
${3:-}
EOF
}
run(){ $CLI run /tmp/p.yaml --output json > /tmp/run.json 2>/tmp/run.err; RC=$?; RUN=$(jq -r '.runId // .id // empty' /tmp/run.json); return 0; }
summ(){ curl -s "$TQ/telemetry/runs/$1/summary" ${JWT:+-H "Authorization: Bearer $JWT"}; }       # -> {requests, errors, rps, p50, p99, p999, ...}
within(){ python3 -c "import sys; a,b,t=map(float,sys.argv[1:]); sys.exit(0 if abs(a-b)<=t*max(abs(b),1e-9) else 1)" "$1" "$2" "$3"; }
summary(){ echo "pass=$PASS fail=$FAIL"; [[ $FAIL == 0 ]]; }
```

## smoke.sh
```bash
. ./lib.sh
docker compose ps --format json | jq -s -e 'map(select(.State!="running"))|length==0' >/dev/null && ok SMK-001 || ko SMK-001 "infra not all running"
gql '{__typename}' | jq -e .data >/dev/null && ok SMK-002-gw || ko SMK-002-gw; [[ $(curl -s -o /dev/null -w '%{http_code}' $TQ/health) == 200 ]] && ok SMK-002-tq || ko SMK-002-tq
$CLI validate ../tools/testforge-cli/examples/pipelines/checkout-load-test.yaml && ok SMK-003 || ko SMK-003
calcfg 'fixed_ms=5'; pipeline 50 10; run; [[ $RC == 0 && -n $RUN ]] && ok SMK-004-run || ko SMK-004-run "$(head -c 300 /tmp/run.err)"
sleep 10; [[ $(summ $RUN | jq '.requests') -gt 0 ]] && ok SMK-004-telemetry || ko SMK-004-telemetry
summary
```

## accuracy.sh (ACC-001..007) — the core product-correctness suite
```bash
. ./lib.sh
for rps in 1 100 5000; do calcfg 'fixed_ms=2'; pipeline $rps 60; run; sleep 15; T=$(truth | jq .total); S=$(summ $RUN | jq .requests); W=$(jq '.stats.requests // .requests' /tmp/run.json)
  [[ $T == $W && $T == $S ]] && ok "ACC-001 count rps=$rps ($T)" || ko "ACC-001 rps=$rps" "target=$T worker=$W telemetry=$S"
  within $T $((rps*60)) 0.01 && ok "ACC-002 mean rate rps=$rps" || ko "ACC-002 mean rate rps=$rps" "sent $T expected $((rps*60))"
  truth | jq -e --argjson r $rps '[.perSecond|to_entries|sort_by(.key)|.[1:-1][]|.value] | all(. >= $r*0.95 and . <= $r*1.05)' >/dev/null && ok "ACC-002 pacing rps=$rps" || ko "ACC-002 pacing rps=$rps" "$(truth | jq -c '[.perSecond[]]|[min,max]')"; done
calcfg 'fixed_ms=50'; pipeline 200 60; run; sleep 15; R=$(summ $RUN); for p in p50 p99; do v=$(jq .$p <<<"$R"); python3 -c "import sys; sys.exit(0 if 50<=$v<=56 else 1)" && ok "ACC-003 fixed $p=$v" || ko "ACC-003 fixed $p" "$v (expect 50..56ms)"; done
calcfg 'fixed_ms=10&slow_pct=5&slow_ms=500'; pipeline 200 120; run; sleep 15; R=$(summ $RUN); within $(jq .p99 <<<"$R") 500 0.06 && within $(jq .p50 <<<"$R") 10 0.5 && ok ACC-003-bimodal || ko ACC-003-bimodal "$(jq -c '{p50,p99}' <<<"$R")"
# ACC-004 coordinated omission: 2 s stall every 10 s at 200 rps open model => ~20% of requests are delayed; p99.9 MUST be >= ~1500 ms
calcfg 'fixed_ms=2&stall_every_s=10&stall_ms=2000'; pipeline 200 120; run; sleep 15; R=$(summ $RUN); python3 -c "import sys; sys.exit(0 if $(jq '.p999 // .p99' <<<"$R")>=1500 else 1)" && ok ACC-004-no-coordinated-omission || ko ACC-004 "p99.9=$(jq .p999 <<<"$R") — tool under-reports tail (coordinated omission)"
calcfg 'fixed_ms=2&err_pct=10&reset_pct=1'; pipeline 500 60; run; sleep 15; R=$(summ $RUN); TR=$(truth); within $(jq '.errors.http5xx // .errors' <<<"$R") $(jq .errors <<<"$TR") 0.02 && ok ACC-006-5xx || ko ACC-006-5xx "$(jq -c .errors <<<"$R") vs truth $(jq .errors <<<"$TR")"
within $(jq '.errors.connectionReset // 0' <<<"$R") $(jq .resets <<<"$TR") 0.1 && ok ACC-006-resets || ko ACC-006-resets
# ACC-005 saturation flag: absurd target on one worker
calcfg 'fixed_ms=0'; pipeline 2000000 20 '    workers: 1'; run; jq -e '.. | .generatorSaturated? // empty | select(.==true)' /tmp/run.json >/dev/null && ok ACC-005 || ko ACC-005 "no saturation warning when achieved << target"
# ACC-007 multi-worker percentile merge: compare with exact percentile from raw events in ClickHouse
calcfg 'fixed_ms=10&slow_pct=2&slow_ms=300'; pipeline 2000 60 '    workers: 8'; run; sleep 20; exact=$(curl -s localhost:8123 --data-binary "SELECT quantileExact(0.99)(latency_ms) FROM telemetry_events WHERE run_id='$RUN'"); within $(summ $RUN | jq .p99) $exact 0.05 && ok ACC-007 || ko ACC-007 "reported $(summ $RUN | jq .p99) exact $exact"
dups=$(curl -s localhost:8123 --data-binary "SELECT count()-uniqExact(event_id) FROM telemetry_events WHERE run_id='$RUN'"); [[ $dups == 0 ]] && ok I5-no-duplicate-events || ko I5 "$dups duplicates"
summary
```

## safety.sh (I1, I2, NEG-002, CHA-003/011) — P0
```bash
. ./lib.sh
for tgt in http://169.254.169.254/latest/meta-data http://localhost:4000/graphql http://127.0.0.1:5432 http://10.0.0.1 http://[::1]:8200 file:///etc/passwd http://not-allow-listed.example.com http://2130706433 http://localhost.attacker.io; do
  printf 'name: ssrf\nstages:\n  - {id: l, type: http-load, target: "%s", rate: {model: open, rps: 5}, duration: 5s}\n' "$tgt" > /tmp/p.yaml; run; [[ $RC != 0 ]] && ok "NEG-002 $tgt" || ko "NEG-002 $tgt" "run accepted against forbidden target"; done
# kill switch: load must stop within 5 s
calcfg 'fixed_ms=1'; pipeline 1000 300; ( run ) & sleep 20; gql 'mutation{activateGlobalKillSwitch(reason:"qa"){active}}' >/dev/null; sleep 5; a=$(truth | jq .total); sleep 10; b=$(truth | jq .total)
[[ $a == $b ]] && ok I2-kill-switch || ko I2-kill-switch "target still received $((b-a)) requests 5-15 s after kill switch"; gql 'mutation{deactivateGlobalKillSwitch{active}}' >/dev/null; wait || true
# dead-man: control plane dies while workers run => workers must stop on their own
calcfg 'fixed_ms=1'; pipeline 500 600; ( run ) & sleep 20; pkill -KILL -f 'dag-workflow-scheduler|pipeline-api-gateway' || docker compose kill dag-workflow-temporal; sleep ${DEADMAN:-60}; a=$(truth | jq .total); sleep 10; b=$(truth | jq .total)
[[ $a == $b ]] && ok CHA-003-deadman || ko CHA-003-deadman "orphaned workers still sending ($((b-a)) req/10s) with control plane dead"; echo "restart control plane now"; wait || true
truth | jq -c .sources; echo "verify every source IP above belongs to the worker fleet of THIS run"
summary
```

## chaos_sidecar_kind.sh (FUN-014, ACC-009, CHA-002, NEG-008, CON-005) — needs Linux + kind
```bash
. ./lib.sh; kind create cluster --name tf-qa || true
kubectl create deploy cal --image=cal:local --replicas=3 --port=9900 || true; kubectl expose deploy cal --port=9900 || true; kubectl create ns bystander || true; kubectl -n bystander create deploy cal --image=cal:local --port=9900 || true
helm upgrade --install chaos ../infrastructure/helm-charts/protocol-load-worker-go -f values-qa.yaml   # plus chaos sidecar manifests from infrastructure/kubernetes/base: VERIFY
probe(){ kubectl run p$RANDOM --rm -i --restart=Never --image=curlimages/curl -q -- sh -c "for i in \$(seq 50); do curl -s -o /dev/null -w '%{time_total}\n' http://$1:9900/; done" 2>/dev/null | awk '{s+=$1}END{printf "%.0f\n", s/NR*1000}'; }
base=$(probe cal.default); byst=$(probe cal.bystander)
gql 'mutation($i:FaultInput!){injectFault(input:$i){id}}' '{"i":{"type":"LATENCY","latencyMs":100,"selector":{"namespace":"default","labels":{"app":"cal"}},"durationSeconds":120}}' | tee /tmp/f.json; sleep 5
f=$(probe cal.default); python3 -c "import sys; sys.exit(0 if 90<=$f-$base<=110 else 1)" && ok ACC-009-latency || ko ACC-009-latency "added $((f-base)) ms (expected 100±10)"
b2=$(probe cal.bystander); python3 -c "import sys; sys.exit(0 if abs($b2-$byst)<10 else 1)" && ok I1-blast-radius || ko I1-blast-radius "bystander namespace affected (+$((b2-byst)) ms)"
kubectl delete pod -l app=chaos-ebpf-sidecar --force --grace-period=0; sleep 140          # CHA-002: injector killed while fault active; wait past fault TTL
r=$(probe cal.default); python3 -c "import sys; sys.exit(0 if abs($r-$base)<10 else 1)" && ok CHA-002-auto-revert || ko CHA-002-auto-revert "fault persists after sidecar death + TTL (+$((r-base)) ms) — P0"
for sel in '{"namespace":"kube-system"}' '{"namespace":"default","labels":{"app":"does-not-exist"}}' '{}'; do gql 'mutation($i:FaultInput!){injectFault(input:$i){id}}' "{\"i\":{\"type\":\"POD_KILL\",\"selector\":$sel,\"durationSeconds\":10}}" | jq -e .errors >/dev/null && ok "NEG-008 $sel" || ko "NEG-008 $sel" "accepted"; done
for n in $(kubectl get nodes -o name); do docker exec ${n#node/} sh -c 'tc qdisc show | grep -c netem; bpftool prog show 2>/dev/null | grep -c chaos' ; done | paste -sd+ - | bc | { read x; [[ $x == 0 ]] && ok CON-005-no-residual-faults || ko CON-005 "$x residual tc/eBPF artefacts"; }
summary
```

## scheduler_and_races.sh (I3, RACE-001/004, FUN-004/005, CHA-004)
```bash
. ./lib.sh; calcfg 'fixed_ms=1'
cat > /tmp/dag.yaml <<EOF
name: dag-$RANDOM
stages:
  - {id: a, type: http-load, target: $CAL/a, rate: {model: open, rps: 10}, duration: 5s}
  - {id: b, type: http-load, target: $CAL/b, rate: {model: open, rps: 10}, duration: 5s, dependsOn: [a]}
  - {id: c, type: http-load, target: $CAL/c, rate: {model: open, rps: 10}, duration: 5s, dependsOn: [a]}
  - {id: gate, type: approval, dependsOn: [b, c]}
  - {id: d, type: http-load, target: $CAL/d, rate: {model: open, rps: 10}, duration: 5s, dependsOn: [gate]}
EOF
PID=$(gql 'mutation($y:String!){createPipeline(yaml:$y){id}}' "$(jq -Rsc '{y:.}' /tmp/dag.yaml)" | jq -r '.data[]|.id')
# RACE-001: 20 concurrent starts with same idempotency key -> one run
seq 20 | xargs -P20 -I{} bash -c ". ./lib.sh; gql 'mutation(\$p:ID!,\$k:String!){startRun(pipelineId:\$p,idempotencyKey:\$k){id}}' '{\"p\":\"$PID\",\"k\":\"fixed-key\"}'" | jq -r '.data[]?.id' | sort -u > /tmp/runs; [[ $(wc -l < /tmp/runs) == 1 ]] && ok RACE-001 || ko RACE-001 "$(wc -l < /tmp/runs) runs"; RUN=$(head -1 /tmp/runs)
st(){ gql 'query($r:ID!){run(id:$r){status nodes{id status startedAt finishedAt attempts}}}' "{\"r\":\"$RUN\"}" | jq -c '.data.run'; }
for i in $(seq 60); do [[ $(st | jq -r '.nodes[]|select(.id=="gate")|.status') == WAITING_APPROVAL ]] && break; sleep 1; done
st | jq -e '(.nodes|map({(.id):.})|add) as $n | ($n.b.startedAt >= $n.a.finishedAt) and ($n.c.startedAt >= $n.a.finishedAt) and ($n.d.status=="PENDING")' >/dev/null && ok I3-ordering-and-gate || ko I3-ordering-and-gate "$(st)"
# RACE-004: approve vs cancel vs second approve concurrently -> exactly one terminal path
( gql 'mutation($r:ID!){approveGate(runId:$r,nodeId:"gate"){status}}' "{\"r\":\"$RUN\"}" & gql 'mutation($r:ID!){cancelRun(id:$r){status}}' "{\"r\":\"$RUN\"}" & gql 'mutation($r:ID!){approveGate(runId:$r,nodeId:"gate"){status}}' "{\"r\":\"$RUN\"}" & wait ) >/dev/null
for i in $(seq 60); do s=$(st | jq -r .status); [[ $s =~ SUCCEEDED|CANCELLED|FAILED ]] && break; sleep 1; done; dreq=$(truth | jq '.total'); echo "final=$s"
st | jq -e 'if .status=="CANCELLED" then (.nodes[]|select(.id=="d")|.status)!="SUCCEEDED" else true end' >/dev/null && ok RACE-004 || ko RACE-004 "cancelled run but node d executed"
st | jq -e '[.nodes[]|.attempts // 1]|all(.<=1)' >/dev/null && ok I3-once || ko I3-once "node executed more than once: $(st)"
# CHA-004 Temporal outage mid-run: no duplicate node execution (CAL per-path truth would need path counters; use attempts + request totals = 4 stages*50)
RUN=$(gql 'mutation($p:ID!){startRun(pipelineId:$p){id}}' "{\"p\":\"$PID\"}" | jq -r '.data[]|.id'); curl -s $CAL/__reset; sleep 3; docker compose stop dag-workflow-temporal; sleep 45; docker compose start dag-workflow-temporal
for i in $(seq 120); do [[ $(st | jq -r '.nodes[]|select(.id=="gate")|.status') == WAITING_APPROVAL ]] && break; sleep 1; done; t=$(truth | jq .total); within $t 150 0.05 && ok CHA-004-no-duplicate-load || ko CHA-004 "target got $t requests, expected 150 (a,b,c × 50)"
summary
```

## wasm_sandbox_suite (NEG-007, I7) — add `services/execution-fleet/wasm-payload-sandbox-runtime/tests/escape.rs`
```rust
// Each WAT module must be rejected at validation or trapped at runtime with a classified error; host must be untouched. Adapt `Sandbox::run(bytes, limits)` to the crate API.
use wasm_payload_sandbox_runtime::*; fn wat(s: &str) -> Vec<u8> { wat::parse_str(s).unwrap() }   // add `wat` dev-dependency
const L: Limits = Limits { fuel: 10_000_000, memory_bytes: 64 << 20, wall_ms: 2_000 };
#[test] fn infinite_loop_is_stopped() { let t = std::time::Instant::now(); let e = Sandbox::run(&wat("(module (func (export \"generate\") (loop br 0)))"), L).unwrap_err(); assert!(matches!(e, SandboxError::FuelExhausted | SandboxError::Timeout)); assert!(t.elapsed().as_millis() < 5_000); }
#[test] fn memory_bomb_is_capped() { assert!(matches!(Sandbox::run(&wat("(module (memory 1) (func (export \"generate\") (drop (memory.grow (i32.const 65535))) (loop br 0)))"), L).unwrap_err(), SandboxError::MemoryLimit | SandboxError::FuelExhausted | SandboxError::Timeout)); }
#[test] fn stack_overflow_traps() { assert!(Sandbox::run(&wat("(module (func $f (export \"generate\") call $f))"), L).is_err()); }
#[test] fn no_host_fs() { let m = wat(r#"(module (import "wasi_snapshot_preview1" "path_open" (func $o (param i32 i32 i32 i32 i32 i64 i64 i32 i32) (result i32))) (memory (export "memory") 1) (data (i32.const 0) "/etc/passwd")
  (func (export "generate") (result i32) (call $o (i32.const 3) (i32.const 0) (i32.const 0) (i32.const 11) (i32.const 0) (i64.const 0) (i64.const 0) (i32.const 0) (i32.const 100))))"#);
  match Sandbox::run(&m, L) { Err(_) => {}, Ok(out) => assert_ne!(out.as_i32(), 0, "path_open succeeded: sandbox has a preopened dir exposing host FS") } }
#[test] fn no_sockets_or_env() { for imp in ["sock_accept", "sock_send", "environ_get"] { let m = format!("(module (import \"wasi_snapshot_preview1\" \"{imp}\" (func (param i32 i32 i32) (result i32))) (func (export \"generate\")))"); let _ = Sandbox::run(&wat(&m).clone(), L); /* must be link error or return errno; assert no env var value/secret appears in output */ } }
#[test] fn malformed_binaries_do_not_panic() { for b in [vec![], b"\0asm".to_vec(), vec![0u8; 1 << 20], { let mut v = b"\0asm\x01\0\0\0".to_vec(); v.extend([0xff; 4096]); v }] { let _ = Sandbox::run(&b, L); } }
#[test] fn deterministic_with_seed() { let m = std::fs::read("tests/fixtures/payload_gen.wasm").unwrap(); assert_eq!(Sandbox::run_seeded(&m, L, 42).unwrap(), Sandbox::run_seeded(&m, L, 42).unwrap()); }
// fuzz/fuzz_targets/validate.rs: fuzz_target!(|d: &[u8]| { let _ = Sandbox::run(d, TIGHT_LIMITS); });
```

## Unit/property tests to add
```go
// dag-workflow-scheduler/internal/scheduling/priority_property_test.go (FUN-004, RACE-009)
func TestPriorityFairness(t *testing.T){ q:=NewQueue(Config{PerTenantCap:5, AgingAfter:30*time.Second}); r:=rand.New(rand.NewSource(1)); var wg sync.WaitGroup
  for i:=0;i<10000;i++{ wg.Add(1); go func(i int){defer wg.Done(); q.Enqueue(Item{ID:i,Tenant:fmt.Sprint(i%20),Priority:i%3,Enq:time.Now()})}(i) }; wg.Wait(); _ = r
  seen:=map[int]bool{}; running:=map[string]int{}; lastPrio:=map[string]int{}
  for q.Len()>0 { it:=q.Dequeue(running); if seen[it.ID]{t.Fatal("duplicate dequeue")}; seen[it.ID]=true; if running[it.Tenant]>=5{t.Fatal("tenant cap violated")}; _ = lastPrio /* within a tenant: higher priority first unless aged */ }
  if len(seen)!=10000 { t.Fatalf("lost items: %d", 10000-len(seen)) } }
// dag-workflow-scheduler/internal/dispatch/idempotency_test.go: deliver same dispatch message 5× (Kafka redelivery) -> worker executes once; result recorded once
// consistency-verification-worker/internal/checker/corpus_test.go: testdata/histories/{ok_*.json,bad_stale_read.json,bad_lost_update.json,bad_dirty_read.json} -> expected verdicts; 100k-op history completes < 60 s or returns Unknown
// protocol-load-worker-go/internal/stats/histogram_test.go: merge(h1..hn).Quantile(q) within precision of exact quantile over concatenated samples; open-model scheduler records intended-start latency (coordinated omission): with a simulated 2 s stall at 100 rps, p99 >= 1.5 s
// protocol-load-worker-go/internal/pacer/pacer_test.go: with fake clock, 1 rps × 60 s emits exactly 60; 0.1 rps × 60 s emits 6; no burst catch-up beyond configured limit
// traffic-shadow-replay-worker/internal/replay/filter_test.go: POST/PUT/PATCH/DELETE dropped by default; Authorization/Cookie headers scrubbed; timing gaps preserved ±5 %
// testforge-cli/internal/pipelineconfig/validate_table_test.go: one fixture per NEG-001 case -> error contains line number; cmd exit code table
```
```ts
// pipeline-api-gateway/src/__tests__/tenant-isolation.spec.ts — for every Query/Mutation/Subscription taking an id: tenant B on tenant A's id → error, never data; subscription for A's run never delivers to B
// run-cost-estimator/src/estimate.property.test.ts (fast-check) — monotonic in rps, duration, workers; never NaN/negative/Infinity; over-budget ⇒ gate=BLOCK
// redpanda-ingestion-pipeline/src/consumer.exactly-once.test.ts — replay same batch twice + crash before commit ⇒ ClickHouse rows unique by event_id; poison message → DLQ, consumer continues
// telemetry-query-api/src/analytics/__tests__/edge.spec.ts — empty run ⇒ percentiles null (not NaN); single sample; SQL-injection strings in filters are parameterised
// canary-rollout-authorizer/src/authorization/decision-table.test.ts — table incl. "insufficient samples ⇒ BLOCK (fail closed)", "baseline also bad ⇒ BLOCK"
// environment-lifecycle-manager/src/__tests__/ttl-race.test.ts — reaper vs run-start on same env with fake timers: never teardown an env with an active lease
```

## IaC checks (INT-011)
```bash
for c in ../infrastructure/helm-charts/*/; do helm lint $c && helm template t $c | kubeconform -strict -ignore-missing-schemas - ; done
for o in ../infrastructure/kubernetes/overlays/*/; do kubectl kustomize $o | kubeconform -strict -ignore-missing-schemas - ; kubectl kustomize $o | kube-linter lint - ; done
for e in ../infrastructure/terraform/environments/*/; do terraform -chdir=$e init -backend=false -input=false && terraform -chdir=$e validate; done; tfsec ../infrastructure/terraform; checkov -d ../infrastructure --quiet
kubectl kustomize ../infrastructure/kubernetes/overlays/production | yq 'select(.kind=="DaemonSet" or .kind=="Deployment") | select(.spec.template.spec.containers[].securityContext.privileged==true) | .metadata.name'   # expect ONLY chaos-ebpf-sidecar
```

## Run order
unit (`pnpm test`, `go test -race ./...`, `cargo test`) → IaC checks → smoke → accuracy → safety → scheduler_and_races → wasm suite + fuzz → chaos_sidecar_kind → platform load (LOAD-001..003 by scripting `startRun` ×500 and `telemetrygen`) → platform chaos (CHA-004..012 via `docker compose stop/pause <service>` during accuracy.sh, re-asserting ACC-001 + I5) → soak with leak sweep (CON-003/005).
