"""窗口归并迟报重算链路：降采样计数口径、迟报审核、交接复盘与下一轮加权合成。"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from app.services.base import DATA_ROOT


@pytest.fixture(autouse=True)
def setup_data_dir():
    with tempfile.TemporaryDirectory() as tmp:
        with patch("app.services.base.DATA_ROOT", Path(tmp)):
            import app.services.downsample as ds
            import app.services.rollup_window as rw
            for mod in (ds, rw):
                mod.DATA_ROOT = Path(tmp)
            yield Path(tmp)


def _write(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def _read(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def _make_task(tmp: Path, task_no: str = "DOWN-LATE", point: str = "SENS-LATE",
               expected: int = 10) -> int:
    from app.store import store
    tid = max((int(r.get("id", 0)) for r in store.rows("downsample")), default=0) + 1
    store.rows("downsample").append({
        "id": tid, "任务编号": task_no, "源序列": point,
        "源采样频率": "10Hz", "目标频率": "1Hz", "聚合方式": "均值",
        "预期样本数": str(expected), "最低有效样本数": "7",
        "输出点数": "0", "压缩比": "0", "任务水位": "0",
        "status": "待执行", "pending": True, "abnormal": False,
    })
    return tid


def _make_rollup(point: str = "SENS-LATE", rollup_no: str = "ROLL-LATE") -> int:
    from app.store import store
    rid = max((int(r.get("id", 0)) for r in store.rows("rollup_window")), default=0) + 1
    store.rows("rollup_window").append({
        "id": rid, "窗口编号": rollup_no, "测点编号": point,
        "窗口类型": "固定窗口", "窗口长度": "10", "滑动步长": "1",
        "聚合指标": "均值", "窗口条数": "0", "归并水位": "0",
        "status": "待归并", "pending": True, "abnormal": False,
    })
    return rid


def _late_scenario(tmp: Path):
    """预期 10、实收 8、剔除 2、有效 6；迟报第 7 个有效点后刚达阈值。"""
    samples = []
    for i in range(8):
        samples.append({
            "sample_id": f"s{i}", "timestamp": f"2026-09-01T10:00:{i:02d}",
            "calibrated_value_V": 10.0 + i,
        })
    _write(tmp / "calibrated" / "SENS-LATE_calibrated.jsonl", samples)
    # 剔除 2 个异常点：剩 6 条有效
    _write(tmp / "filtered_series" / "SENS-LATE_filtered.jsonl", samples[:6])
    return samples


class TestDownsampleCounters:
    def test_result_carries_valid_expected_and_denominator(self, setup_data_dir):
        from app.services.downsample import DownsampleService

        samples = []
        for i in range(8):
            samples.append({"sample_id": f"s{i}", "timestamp": f"2026-09-01T10:00:{i:02d}",
                            "calibrated_value_V": 10.0 + i})
        _write(setup_data_dir / "calibrated" / "SENS-A_calibrated.jsonl", samples)
        _write(setup_data_dir / "filtered_series" / "SENS-A_filtered.jsonl", samples[:6])
        tid = _make_task(setup_data_dir, "DOWN-A", "SENS-A")
        result = DownsampleService().downsample_series(tid)
        assert "error" not in result
        rows = _read(setup_data_dir / "downsampled" / "DOWN-A_downsampled.jsonl")
        assert len(rows) == 1
        row = rows[0]
        assert row["received_count"] == 8
        assert row["excluded_count"] == 2
        assert row["valid_count"] == 6
        assert row["expected_count"] == 10
        assert row["denominator"] == 6
        assert row["threshold_met"] is False
        assert row["eligible"] is False

    def test_late_sample_makes_window_barely_eligible_and_is_idempotent(self, setup_data_dir):
        from app.services.downsample import DownsampleService

        _late_scenario(setup_data_dir)
        tid = _make_task(setup_data_dir, "DOWN-L", "SENS-LATE")
        svc = DownsampleService()
        svc.downsample_series(tid)

        first = svc.apply_late_sample(tid, 1, {"sample_id": "late-7", "timestamp":
                                                "2026-09-01T10:00:07",
                                                "value": 16.0})
        assert first["deduplicated"] is False
        assert first["valid_count"] == 7
        assert first["expected_count"] == 10
        assert first["denominator"] == 7
        assert first["eligible"] is True
        assert first["threshold_met"] is True

        again = svc.apply_late_sample(tid, 1, {"sample_id": "late-7",
                                                "timestamp": "2026-09-01T10:00:07",
                                                "value": 16.0})
        assert again["deduplicated"] is True
        assert again["valid_count"] == 7
        assert again["denominator"] == 7
        # 文件里仍然只有一个窗口
        rows = _read(setup_data_dir / "downsampled" / "DOWN-L_downsampled.jsonl")
        assert len(rows) == 1
        assert rows[0]["valid_count"] == 7


class TestRollupLateCorrection:
    def test_rollup_started_then_late_correction_refreshes_same_round(self, setup_data_dir):
        from app.services.downsample import DownsampleService
        from app.services.rollup_window import RollupWindowService
        from app.store import store

        _late_scenario(setup_data_dir)
        tid = _make_task(setup_data_dir, "DOWN-L", "SENS-LATE")
        DownsampleService().downsample_series(tid)
        rid = _make_rollup()

        svc = RollupWindowService()
        # 归并开工：窗口只有 6 个有效点，不合格，不采用
        started = svc.rollup_windows(rid)
        assert started["round"] == 1
        assert started["window_period_count"] == 0
        assert started["committed"] is False

        late = svc.submit_late_correction(rid, task_id="DOWN-L", window_index=1,
                                          sample={"sample_id": "late-7",
                                                  "timestamp": "2026-09-01T10:00:07",
                                                  "value": 16.0})
        assert late["review_status"] == "待审核"
        assert late["valid_count"] == 7

        # 未审核前不能提交/交接
        blocked = svc.create_handoff(rid)
        assert "error" in blocked
        assert blocked["pending_reviews"]

        reviewed = svc.review_late_sample(rid, window_key=late["window_key"],
                                          sample_id="late-7")
        assert reviewed["reviewed_count"] == 1
        assert reviewed["reviews"][0]["eligible"] is True

        # 重算仍停留在第 1 轮，窗口刚达阈值被采用
        refreshed = svc.rollup_windows(rid)
        assert refreshed["round"] == 1
        assert refreshed["window_period_count"] == 1
        assert refreshed["denominator"] == 7
        assert refreshed["committed"] is True
        assert store.find("rollup_window", rid)["status"] == "已归并"

    def test_handoff_freezes_windows_and_replays_original(self, setup_data_dir):
        from app.services.downsample import DownsampleService
        from app.services.rollup_window import RollupWindowService

        _late_scenario(setup_data_dir)
        tid = _make_task(setup_data_dir, "DOWN-L", "SENS-LATE")
        DownsampleService().downsample_series(tid)
        rid = _make_rollup()
        svc = RollupWindowService()
        svc.rollup_windows(rid)
        svc.submit_late_correction(rid, task_id="DOWN-L", window_index=1,
                                   sample={"sample_id": "late-7",
                                           "timestamp": "2026-09-01T10:00:07",
                                           "value": 16.0})
        svc.review_late_sample(rid, sample_id="late-7")
        svc.rollup_windows(rid)

        handoff = svc.create_handoff(rid)
        assert "error" not in handoff
        assert handoff["windows"][0]["valid_count"] == 7
        assert handoff["windows"][0]["expected_count"] == 10
        assert handoff["denominator"] == 7

        replay = svc.replay_handoff(rid, handoff["handoff_id"])
        assert replay["restorable"] is True
        assert replay["window_keys"] == [handoff["windows"][0]["window_key"]]
        assert replay["denominator"] == 7
        # 复盘值与原交接一致
        assert replay["replayed_value"] == handoff["combined_value"]

    def test_next_round_merges_second_window_by_valid_counts(self, setup_data_dir):
        """两窗有效点数 7 / 10，窗均值 20 / 37 → 合并值 30。"""
        from app.services.downsample import DownsampleService
        from app.services.rollup_window import RollupWindowService
        from app.store import store

        # 窗口 A：7 个有效点，均值 20
        a = [{"sample_id": f"a{i}", "timestamp": f"2026-09-01T10:00:{i:02d}",
              "calibrated_value_V": 20.0} for i in range(7)]
        _write(setup_data_dir / "filtered_series" / "SENS-M_filtered.jsonl", a)
        _write(setup_data_dir / "calibrated" / "SENS-M_calibrated.jsonl", a)
        tid = _make_task(setup_data_dir, "DOWN-M", "SENS-M", expected=7)
        DownsampleService().downsample_series(tid)

        rid = _make_rollup(point="SENS-M", rollup_no="ROLL-M")
        svc = RollupWindowService()
        first = svc.rollup_windows(rid)
        assert first["denominator"] == 7
        assert first["combined_value"] == 20.0
        first_handoff = svc.create_handoff(rid)

        # 另一个合格窗：10 个有效点，均值 37
        store.rows("downsample").append({
            "id": max(int(r.get("id", 0)) for r in store.rows("downsample")) + 1,
            "任务编号": "DOWN-M2", "源序列": "SENS-M2",
            "源采样频率": "10Hz", "目标频率": "1Hz", "聚合方式": "均值",
            "预期样本数": "10", "最低有效样本数": "7",
            "输出点数": "0", "压缩比": "0", "任务水位": "0",
            "status": "待执行", "pending": True, "abnormal": False,
        })
        b = [{"sample_id": f"b{i}", "timestamp": f"2026-09-01T11:00:{i:02d}",
              "calibrated_value_V": 37.0} for i in range(10)]
        _write(setup_data_dir / "filtered_series" / "SENS-M2_filtered.jsonl", b)
        _write(setup_data_dir / "calibrated" / "SENS-M2_calibrated.jsonl", b)
        tid2 = max(int(r["id"]) for r in store.rows("downsample"))
        DownsampleService().downsample_series(tid2)

        second = svc.next_round(rid, include_window_keys=["DOWN-M2:1"])
        assert second["round"] == 2
        assert second["window_period_count"] == 2
        assert second["denominator"] == 17
        assert second["combined_value"] == 30.0
        # 每窗的有效点数被关联保存
        counts = {w["window_key"]: w["valid_count"] for w in second["windows"]}
        assert counts == {"DOWN-M:1": 7, "DOWN-M2:1": 10}

        # 旧交接仍能还原当时只采用窗口 A
        replay = svc.replay_handoff(rid, first_handoff["handoff_id"])
        assert replay["window_keys"] == ["DOWN-M:1"]
        assert replay["denominator"] == 7
        assert replay["combined_value"] == 20.0

    def test_duplicate_late_sample_keeps_denominator_and_old_handoff(self, setup_data_dir):
        from app.services.downsample import DownsampleService
        from app.services.rollup_window import RollupWindowService
        from app.store import store

        # 窗口 A：实收 8、剔除 2 → 6 个有效点；迟报第 7 点后刚达阈值
        a = [{"sample_id": f"a{i}", "timestamp": f"2026-09-01T10:00:{i:02d}",
              "calibrated_value_V": 20.0} for i in range(8)]
        _write(setup_data_dir / "filtered_series" / "SENS-D_filtered.jsonl", a[:6])
        _write(setup_data_dir / "calibrated" / "SENS-D_calibrated.jsonl", a)
        tid = _make_task(setup_data_dir, "DOWN-D", "SENS-D", expected=10)
        DownsampleService().downsample_series(tid)
        rid = _make_rollup(point="SENS-D", rollup_no="ROLL-D")
        svc = RollupWindowService()
        svc.rollup_windows(rid)

        late = {"sample_id": "late-dup", "timestamp": "2026-09-01T10:00:09",
                "value": 20.0}
        svc.submit_late_correction(rid, task_id="DOWN-D", window_index=1, sample=late)
        svc.review_late_sample(rid, sample_id="late-dup")
        svc.rollup_windows(rid)
        handoff1 = svc.create_handoff(rid)
        assert handoff1["denominator"] == 7

        store.rows("downsample").append({
            "id": max(int(r.get("id", 0)) for r in store.rows("downsample")) + 1,
            "任务编号": "DOWN-D2", "源序列": "SENS-D2",
            "源采样频率": "10Hz", "目标频率": "1Hz", "聚合方式": "均值",
            "预期样本数": "10", "最低有效样本数": "7",
            "输出点数": "0", "压缩比": "0", "任务水位": "0",
            "status": "待执行", "pending": True, "abnormal": False,
        })
        b = [{"sample_id": f"b{i}", "timestamp": f"2026-09-01T11:00:{i:02d}",
              "calibrated_value_V": 37.0} for i in range(10)]
        _write(setup_data_dir / "filtered_series" / "SENS-D2_filtered.jsonl", b)
        _write(setup_data_dir / "calibrated" / "SENS-D2_calibrated.jsonl", b)
        tid2 = max(int(r["id"]) for r in store.rows("downsample"))
        DownsampleService().downsample_series(tid2)
        second = svc.next_round(rid, include_window_keys=["DOWN-D2:1"])
        assert second["denominator"] == 17
        svc.create_handoff(rid)

        # 再提交同一迟报点：幂等去重，下一轮分母仍为 17
        dup = svc.submit_late_correction(rid, task_id="DOWN-D", window_index=1, sample=late)
        assert dup["deduplicated"] is True
        after = svc.rollup_windows(rid)
        assert after["round"] == 3
        assert after["denominator"] == 17
        assert after["combined_value"] == 30.0

        # 第三次重复提交：仍为 17，旧交接可复盘
        dup2 = svc.submit_late_correction(rid, task_id="DOWN-D", window_index=1, sample=late)
        assert dup2["deduplicated"] is True
        again = svc.rollup_windows(rid)
        assert again["denominator"] == 17
        replay = svc.replay_handoff(rid, handoff1["handoff_id"])
        assert replay["window_keys"] == ["DOWN-D:1"]
        assert replay["denominator"] == 7
        assert replay["combined_value"] == 20.0

    def test_rejected_late_sample_rolls_back_counters(self, setup_data_dir):
        from app.services.downsample import DownsampleService
        from app.services.rollup_window import RollupWindowService

        _late_scenario(setup_data_dir)
        tid = _make_task(setup_data_dir, "DOWN-L", "SENS-LATE")
        DownsampleService().downsample_series(tid)
        rid = _make_rollup()
        svc = RollupWindowService()
        svc.rollup_windows(rid)
        svc.submit_late_correction(rid, task_id="DOWN-L", window_index=1,
                                   sample={"sample_id": "late-7",
                                           "timestamp": "2026-09-01T10:00:07",
                                           "value": 16.0})
        window = svc._load_windows("SENS-LATE")[0]
        assert (window["valid_count"], window["eligible"]) == (7, True)

        rejected = svc.review_late_sample(rid, sample_id="late-7", approve=False)
        assert rejected["reviews"][0]["reverted"] is True
        assert rejected["reviews"][0]["after_revert"]["valid_count"] == 6
        window = svc._load_windows("SENS-LATE")[0]
        assert (window["valid_count"], window["denominator"], window["eligible"]) == (6, 6, False)

        # 驳回后窗口不合格，本轮不应采用，交接也会被拒绝
        refreshed = svc.rollup_windows(rid)
        assert refreshed["window_period_count"] == 0
        assert "error" in svc.create_handoff(rid)

    def test_rejected_sample_can_be_resubmitted_once(self, setup_data_dir):
        from app.services.downsample import DownsampleService
        from app.services.rollup_window import RollupWindowService

        _late_scenario(setup_data_dir)
        tid = _make_task(setup_data_dir, "DOWN-L", "SENS-LATE")
        DownsampleService().downsample_series(tid)
        rid = _make_rollup()
        svc = RollupWindowService()
        svc.rollup_windows(rid)
        late = {"sample_id": "late-7", "timestamp": "2026-09-01T10:00:07", "value": 16.0}
        svc.submit_late_correction(rid, task_id="DOWN-L", window_index=1, sample=late)
        svc.review_late_sample(rid, sample_id="late-7", approve=False)
        again = svc.submit_late_correction(rid, task_id="DOWN-L", window_index=1, sample=late)
        assert again["deduplicated"] is False
        assert again["valid_count"] == 7
        svc.review_late_sample(rid, sample_id="late-7", approve=True)
        refreshed = svc.rollup_windows(rid)
        assert refreshed["denominator"] == 7
        assert refreshed["window_period_count"] == 1

    def test_rollup_output_file_holds_only_latest_round(self, setup_data_dir):
        from app.services.downsample import DownsampleService
        from app.services.rollup_window import RollupWindowService
        from app.store import store

        a = [{"sample_id": f"a{i}", "timestamp": f"2026-09-01T10:00:{i:02d}",
              "calibrated_value_V": 20.0} for i in range(7)]
        _write(setup_data_dir / "filtered_series" / "SENS-R_filtered.jsonl", a)
        _write(setup_data_dir / "calibrated" / "SENS-R_calibrated.jsonl", a)
        tid = _make_task(setup_data_dir, "DOWN-R", "SENS-R", expected=7)
        DownsampleService().downsample_series(tid)
        rid = _make_rollup(point="SENS-R", rollup_no="ROLL-R")
        svc = RollupWindowService()
        svc.rollup_windows(rid)
        svc.create_handoff(rid)
        svc.next_round(rid)

        rows = _read(setup_data_dir / "rollup_windows" / "ROLL-R_rollup.jsonl")
        keys = [row["window_key"] for row in rows]
        assert keys == list(dict.fromkeys(keys))
        assert len(rows) == 1
        # 历史轮次仍可从状态文件复盘
        state = json.loads(
            (setup_data_dir / "rollup_windows" / "ROLL-R_state.json").read_text("utf-8"))
        assert len(state["rounds"]) == 2
        assert len(state["handoffs"]) == 1

    def test_no_window_counted_twice_across_rounds(self, setup_data_dir):
        from app.services.downsample import DownsampleService
        from app.services.rollup_window import RollupWindowService

        a = [{"sample_id": f"a{i}", "timestamp": f"2026-09-01T10:00:{i:02d}",
              "calibrated_value_V": 20.0} for i in range(7)]
        _write(setup_data_dir / "filtered_series" / "SENS-T_filtered.jsonl", a)
        _write(setup_data_dir / "calibrated" / "SENS-T_calibrated.jsonl", a)
        tid = _make_task(setup_data_dir, "DOWN-T", "SENS-T", expected=7)
        DownsampleService().downsample_series(tid)
        rid = _make_rollup(point="SENS-T", rollup_no="ROLL-T")
        svc = RollupWindowService()
        svc.rollup_windows(rid)
        svc.create_handoff(rid)

        nxt = svc.next_round(rid)
        assert nxt["denominator"] == 7
        keys = nxt["window_keys"]
        assert len(keys) == len(set(keys))
