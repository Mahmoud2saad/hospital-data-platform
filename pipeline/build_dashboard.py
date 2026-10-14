"""Build docs/dashboard.html - a self-contained (no CDN, no server) dashboard over the gold layer."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent

TEMPLATE = r"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Meridian Health Group - Operations Dashboard (synthetic data)</title>
<style>
:root{--bg:#f8fafc;--card:#fff;--ink:#0f172a;--mut:#64748b;--line:#e2e8f0;--blue:#2563eb;--amber:#d97706;--red:#dc2626;--grey:#94a3b8}
@media(prefers-color-scheme:dark){:root{--bg:#0b1220;--card:#111a2e;--ink:#e5e7eb;--mut:#94a3b8;--line:#1f2a44;--blue:#60a5fa;--amber:#fbbf24;--red:#f87171;--grey:#64748b}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.45 system-ui,-apple-system,Segoe UI,sans-serif}
header{padding:20px 24px 4px}h1{margin:0;font-size:20px}header p{margin:4px 0 0;color:var(--mut);font-size:12.5px}
.grid{display:grid;gap:14px;padding:16px 24px 28px;grid-template-columns:repeat(12,1fr)}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px 16px}
.kpi{grid-column:span 3}.kpi b{display:block;font-size:26px;letter-spacing:-.5px}.kpi span{color:var(--mut);font-size:12px}
.w4{grid-column:span 4}.w6{grid-column:span 6}.w8{grid-column:span 8}.w12{grid-column:span 12}
h2{margin:0 0 8px;font-size:13px;color:var(--mut);font-weight:600;text-transform:uppercase;letter-spacing:.04em}
svg{width:100%;height:auto;display:block}text{fill:var(--mut);font-size:11px}.v{fill:var(--ink)}
table{border-collapse:collapse;width:100%;font-size:12.5px}td,th{padding:5px 8px;text-align:right;border-bottom:1px solid var(--line)}th:first-child,td:first-child{text-align:left}
th{color:var(--mut);font-weight:600}.hint{color:var(--mut);font-size:12px;margin-top:6px}
select{background:var(--card);color:var(--ink);border:1px solid var(--line);border-radius:6px;padding:3px 6px}
@media(max-width:900px){.kpi{grid-column:span 6}.w4,.w6,.w8{grid-column:span 12}}
</style></head><body>
<header><h1>Meridian Health Group - Operations Dashboard</h1>
<p>Built from the gold layer. <b>Synthetic data</b> - every number is randomly generated; no real patients or billing. Data as of __ASOF__.</p></header>
<div class="grid" id="g">
<div class="card kpi"><span>Valid inpatient admissions</span><b id="k1"></b></div>
<div class="card kpi"><span>30-day unplanned readmission</span><b id="k2"></b></div>
<div class="card kpi"><span>Net revenue (SAR, ex VAT)</span><b id="k3"></b></div>
<div class="card kpi"><span>DQ exceptions (HIGH / total)</span><b id="k4"></b></div>
<div class="card w4"><h2>Readmission rate by branch</h2><svg id="c1" viewBox="0 0 360 220"></svg></div>
<div class="card w8"><h2>Revenue: service month vs transaction month (SAR m)</h2><svg id="c2" viewBox="0 0 700 220"></svg>
<div class="hint">Same transactions, two date roles. The gap is billing lag, late invoices, credit notes and adjustments (see the reconciliation bridge).</div></div>
<div class="card w6"><h2>Readmission by department <select id="br"></select></h2><table id="t1"></table></div>
<div class="card w6"><h2>Exceptions caught by the pipeline</h2><svg id="c3" viewBox="0 0 520 300"></svg></div>
<div class="card w12"><h2>Physicians with the highest readmission rate (min 25 eligible discharges)</h2><table id="t2"></table></div>
</div>
<script>
const D=__DATA__;const $=i=>document.getElementById(i);const f=(n,d=0)=>n.toLocaleString("en-US",{maximumFractionDigits:d});
const ns="http://www.w3.org/2000/svg";function el(p,t,a,x){const e=document.createElementNS(ns,t);for(const k in a)e.setAttribute(k,a[k]);if(x!=null)e.textContent=x;p.appendChild(e);return e}
$("k1").textContent=f(D.kpi.valid);$("k2").textContent=D.kpi.rate.toFixed(1)+"%";$("k3").textContent="SAR "+f(D.kpi.revenue/1e6,1)+"m";$("k4").textContent=D.kpi.high+" / "+D.kpi.dq;
(function(){const s=$("c1"),m=D.branch,mx=Math.max(...m.map(r=>r.rate))*1.2;m.forEach((r,i)=>{const y=12+i*40;el(s,"text",{x:0,y:y+17},r.branch);const w=r.rate/mx*220;el(s,"rect",{x:80,y:y,width:w,height:26,rx:4,fill:"var(--blue)"});el(s,"text",{x:86+w,y:y+17,class:"v"},r.rate.toFixed(1)+"%  ("+r.num+"/"+r.den+")")});
const gx=80+D.kpi.rate/mx*220;el(s,"line",{x1:gx,x2:gx,y1:4,y2:212,stroke:"var(--amber)","stroke-dasharray":"4 3"});el(s,"text",{x:gx+3,y:216,style:"fill:var(--amber)"},"group "+D.kpi.rate.toFixed(1)+"%")})();
(function(){const s=$("c2"),r=D.rev,L=40,R=690,T=10,B=190,mx=Math.max(...r.map(x=>Math.max(x.svc,x.trx)))*1.15;const X=i=>L+i*(R-L)/(r.length-1),Y=v=>B-v/mx*(B-T);
for(let k=0;k<=4;k++){const v=mx*k/4,y=Y(v);el(s,"line",{x1:L,x2:R,y1:y,y2:y,stroke:"var(--line)"});el(s,"text",{x:L-6,y:y+4,"text-anchor":"end"},v.toFixed(1))}
r.forEach((x,i)=>{if(i%2==0||r.length<10)el(s,"text",{x:X(i),y:206,"text-anchor":"middle"},x.m)});
[["svc","var(--blue)"],["trx","var(--amber)"]].forEach(([k,c])=>{el(s,"polyline",{points:r.map((x,i)=>X(i)+","+Y(x[k])).join(" "),fill:"none",stroke:c,"stroke-width":2});r.forEach((x,i)=>{const d=el(s,"circle",{cx:X(i),cy:Y(x[k]),r:3,fill:c});el(d,"title",{},x.m+" "+(k=="svc"?"service":"transaction")+": "+x[k].toFixed(2)+"m")})});
el(s,"text",{x:L+4,y:T+8,style:"fill:var(--blue)"},"by service month");el(s,"text",{x:L+110,y:T+8,style:"fill:var(--amber)"},"by transaction month")})();
const sel=$("br");["All branches",...D.branch.map(b=>b.branch)].forEach(b=>{const o=document.createElement("option");o.textContent=b;sel.appendChild(o)});
function t1(){const b=sel.value,rows={};D.dept.filter(r=>b=="All branches"||r.branch==b).forEach(r=>{const o=rows[r.dept]||(rows[r.dept]={den:0,num:0});o.den+=r.den;o.num+=r.num});
$("t1").innerHTML="<tr><th>Department</th><th>Eligible</th><th>Readmitted</th><th>Rate</th></tr>"+Object.entries(rows).sort((a,b)=>b[1].num/b[1].den-a[1].num/a[1].den).map(([d,o])=>`<tr><td>${d}</td><td>${f(o.den)}</td><td>${o.num}</td><td>${(100*o.num/o.den).toFixed(1)}%</td></tr>`).join("")}
sel.onchange=t1;t1();
(function(){const s=$("c3"),m=D.dq,mx=Math.max(...m.map(r=>r.n)),col={HIGH:"var(--red)",MEDIUM:"var(--amber)",LOW:"var(--grey)"};m.forEach((r,i)=>{const y=6+i*24;el(s,"text",{x:0,y:y+13},r.rule.replace(/_/g," ").toLowerCase());const w=r.n/mx*210;el(s,"rect",{x:250,y:y,width:w,height:17,rx:3,fill:col[r.sev]});el(s,"text",{x:256+w,y:y+13,class:"v"},r.n)})})();
$("t2").innerHTML="<tr><th>Physician</th><th>Branch</th><th>Department</th><th>Eligible</th><th>Readmitted</th><th>Rate</th></tr>"+D.phys.map(r=>`<tr><td>${r.name}</td><td>${r.branch}</td><td>${r.dept}</td><td>${r.den}</td><td>${r.num}</td><td>${r.rate.toFixed(1)}%</td></tr>`).join("");
</script></body></html>"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", type=Path, default=ROOT / "output" / "warehouse.duckdb")
    ap.add_argument("--out", type=Path, default=ROOT / "docs" / "dashboard.html")
    a = ap.parse_args()
    c = duckdb.connect(str(a.db), read_only=True)
    q = lambda s, *p: c.execute(s, list(p)).fetchall()
    run = q("SELECT MAX(run_id) FROM ctl.dq_exceptions")[0][0]
    grp = q("SELECT eligible_index_admissions, unplanned_readmissions, readmission_rate_pct FROM gold.vw_readmission_rate_group")[0]
    sev = dict(q("SELECT severity, COUNT(*) FROM ctl.dq_exceptions WHERE run_id=? GROUP BY 1", run))
    rev = q("""SELECT s.revenue_month, s.service_period_revenue, t.transaction_period_revenue
               FROM gold.vw_revenue_service_period s JOIN gold.vw_revenue_transaction_period t USING (revenue_month) ORDER BY 1""")[1:-1]
    data = {
        "kpi": {"valid": int(q("SELECT SUM(is_valid_admission) FROM gold.fact_admission")[0][0]), "rate": float(grp[2]),
                "revenue": float(q("SELECT SUM(AmountExVAT) FROM gold.fact_billing_transaction")[0][0]),
                "high": sev.get("HIGH", 0), "dq": sum(sev.values())},
        "branch": [dict(branch=r[0], rate=float(r[3]), num=int(r[2]), den=int(r[1])) for r in
                   q("SELECT * FROM gold.vw_readmission_rate_by_branch ORDER BY readmission_rate_pct DESC")],
        "dept": [dict(branch=r[0], dept=r[1], den=int(r[2]), num=int(r[3])) for r in
                 q("SELECT Branch, Department, eligible_index_admissions, unplanned_readmissions FROM gold.vw_readmission_rate_by_department")],
        "rev": [dict(m=r[0].strftime("%b %y"), svc=float(r[1]) / 1e6, trx=float(r[2]) / 1e6) for r in rev],
        "dq": [dict(rule=r[0], sev=r[1], n=int(r[2])) for r in
               q("SELECT rule_code, severity, COUNT(*) n FROM ctl.dq_exceptions WHERE run_id=? GROUP BY 1,2 ORDER BY n DESC", run)],
        "phys": [dict(name=r[1], branch=r[2], dept=r[3], den=int(r[4]), num=int(r[5]), rate=float(r[6])) for r in
                 q("SELECT * FROM gold.vw_readmission_rate_by_physician WHERE eligible_index_admissions >= 25 "
                   "AND DoctorNaturalKey <> 'UNKNOWN' ORDER BY readmission_rate_pct DESC LIMIT 8")],
    }
    asof = q("SELECT MAX(as_of_ts) FROM gold.fact_readmission_index")[0][0]
    html = TEMPLATE.replace("__DATA__", json.dumps(data)).replace("__ASOF__", str(asof)[:10])
    a.out.write_text(html)
    print(f"wrote {a.out} ({len(html) // 1024} KB)")


if __name__ == "__main__":
    main()
