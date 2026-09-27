from __future__ import annotations

from bet_lifecycle.demo import run_demo


async def test_demo_lifecycle_runs_and_reconciles() -> None:
    lines: list[str] = []
    result = await run_demo(lines.append)
    assert result["reconciliation_ok"] is True
    assert result["balances"]["p1"]["cash"] == 122_00
    assert any("requote" in line for line in lines)
    assert any("balances unchanged: True" in line for line in lines)
