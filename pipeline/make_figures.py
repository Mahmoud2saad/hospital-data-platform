"""Render docs/img/summary.png from the gold layer (readmission, revenue timing, data-quality catch)."""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent.parent
INK, MUTED, BLUE, ORANGE, GRID = "#1f2937", "#6b7280", "#2563eb", "#d97706", "#e5e7eb"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", type=Path, default=ROOT / "output" / "warehouse.duckdb")
    ap.add_argument("--out", type=Path, default=ROOT / "docs" / "img" / "summary.png")
    a = ap.parse_args()
    con = duckdb.connect(str(a.db), read_only=True)
    run = con.execute("SELECT MAX(run_id) FROM ctl.dq_exceptions").fetchone()[0]

    br = con.execute("SELECT Branch, readmission_rate_pct, eligible_index_admissions FROM gold.vw_readmission_rate_by_branch ORDER BY 2").fetchall()
    grp = con.execute("SELECT readmission_rate_pct FROM gold.vw_readmission_rate_group").fetchone()[0]
    rev = con.execute("""SELECT s.revenue_month, s.service_period_revenue/1e6, t.transaction_period_revenue/1e6
                         FROM gold.vw_revenue_service_period s JOIN gold.vw_revenue_transaction_period t USING (revenue_month)
                         ORDER BY 1""").fetchall()[1:-1]          # drop the partial first / last months
    dq = con.execute("SELECT rule_code, severity, COUNT(*) FROM ctl.dq_exceptions WHERE run_id = ? GROUP BY 1,2 ORDER BY 3", [run]).fetchall()

    plt.rcParams.update({"font.family": "DejaVu Sans", "axes.edgecolor": GRID, "axes.labelcolor": MUTED,
                         "xtick.color": MUTED, "ytick.color": MUTED, "axes.spines.top": False, "axes.spines.right": False})
    fig, ax = plt.subplots(1, 3, figsize=(16, 4.6), gridspec_kw={"width_ratios": [1, 1.35, 1.25]})

    ax[0].barh([b[0] for b in br], [b[1] for b in br], color=BLUE, height=0.6)
    ax[0].axvline(grp, color=ORANGE, lw=1.5, ls="--")
    ax[0].text(grp + 0.1, -0.45, f"group {grp:.1f}%", color=ORANGE, fontsize=9, va="center")
    for i, b in enumerate(br):
        ax[0].text(b[1] + 0.1, i, f"{b[1]:.1f}%", va="center", fontsize=9, color=INK)
    ax[0].set_title("30-day unplanned readmission rate", loc="left", color=INK, fontsize=11, fontweight="bold")
    ax[0].set_xlim(0, max(b[1] for b in br) * 1.25)
    ax[0].xaxis.grid(True, color=GRID); ax[0].set_axisbelow(True)

    x = [r[0].strftime("%b %y") for r in rev]
    ax[1].plot(x, [r[1] for r in rev], color=BLUE, lw=2, marker="o", ms=4, label="by service month")
    ax[1].plot(x, [r[2] for r in rev], color=ORANGE, lw=2, marker="o", ms=4, label="by transaction month")
    ax[1].set_title("Revenue (SAR m): same data, two date roles", loc="left", color=INK, fontsize=11, fontweight="bold")
    ax[1].set_ylim(0, max(max(r[1], r[2]) for r in rev) * 1.2)
    ax[1].legend(frameon=False, fontsize=9, loc="lower left", ncol=2)
    ax[1].yaxis.grid(True, color=GRID); ax[1].set_axisbelow(True)
    ax[1].tick_params(axis="x", rotation=45, labelsize=8)

    col = {"HIGH": "#dc2626", "MEDIUM": "#d97706", "LOW": "#9ca3af"}
    ax[2].barh([d[0].replace("_", " ").title() for d in dq], [d[2] for d in dq], color=[col[d[1]] for d in dq], height=0.65)
    for i, d in enumerate(dq):
        ax[2].text(d[2] + 1, i, str(d[2]), va="center", fontsize=8.5, color=INK)
    ax[2].set_title("Exceptions caught by the pipeline", loc="left", color=INK, fontsize=11, fontweight="bold")
    ax[2].tick_params(axis="y", labelsize=8)
    ax[2].set_xlim(0, max(d[2] for d in dq) * 1.15)
    ax[2].legend(handles=[plt.Rectangle((0, 0), 1, 1, color=c) for c in col.values()], labels=list(col), frameon=False,
                 fontsize=8, loc="lower right", title="severity", title_fontsize=8)
    fig.tight_layout()
    a.out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(a.out, dpi=130)
    print(f"wrote {a.out}")


if __name__ == "__main__":
    main()
