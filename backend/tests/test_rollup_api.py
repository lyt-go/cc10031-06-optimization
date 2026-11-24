"""归并窗口领域操作的 HTTP 契约：迟报更正、审核、交接、复盘与下一轮。

当前环境没装 starlette TestClient 依赖的 httpx，这里直接调用路由函数，
覆盖参数透传、未知操作与错误到 400 的映射。
"""
from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi import HTTPException


@pytest.fixture(autouse=True)
def setup_data_dir():
    with tempfile.TemporaryDirectory() as tmp:
        with patch("app.services.base.DATA_ROOT", Path(tmp)):
            import app.services.downsample as ds
            import app.services.rollup_window as rw
            for mod in (ds, rw):
                mod.DATA_ROOT = Path(tmp)
            yield Path(tmp)


def _rollup(rollup_no: str = "ROLL-API", point: str = "SENS-API") -> int:
    from app.store import store
    rid = max((int(r.get("id", 0)) for r in store.rows("rollup_window")), default=0) + 1
    store.rows("rollup_window").append({
        "id": rid, "窗口编号": rollup_no, "测点编号": point,
        "窗口类型": "固定窗口", "窗口长度": "10", "滑动步长": "1",
        "聚合指标": "均值", "窗口条数": "0", "归并水位": "0",
        "status": "待归并", "pending": True, "abnormal": False,
    })
    return rid


def test_unknown_operation_is_404():
    from app.routers.resource import run_rollup_operation
    with pytest.raises(HTTPException) as exc:
        run_rollup_operation(1, "nope", {})
    assert exc.value.status_code == 404


def test_handoff_without_round_is_400():
    from app.routers.resource import run_rollup_operation
    rid = _rollup()
    with pytest.raises(HTTPException) as exc:
        run_rollup_operation(rid, "handoff", {})
    assert exc.value.status_code == 400
    assert "无法交接" in exc.value.detail


def test_next_round_and_summary_dispatch():
    from app.routers.resource import run_rollup_operation
    rid = _rollup()
    run = run_rollup_operation(rid, "next-round", {})
    assert run["rollup_id"] == "ROLL-API"
    assert run["round"] == 1
    summary = run_rollup_operation(rid, "summary", {})
    assert summary["rollup_id"] == "ROLL-API"


def test_late_sample_without_target_is_400():
    from app.routers.resource import run_rollup_operation
    rid = _rollup("ROLL-API2", "SENS-API2")
    with pytest.raises(HTTPException) as exc:
        run_rollup_operation(rid, "late-sample",
                             {"sample": {"sample_id": "x", "value": 1.0}})
    assert exc.value.status_code == 400
