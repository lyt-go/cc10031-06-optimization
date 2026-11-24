"""窗口归并领域服务：维护固定、滑动与会话窗口的时序归并聚合指标与窗口条数。

归并消费降采样结果，并按降采样窗口携带的有效样本数、预期样本数和计入均值的分母
核实窗口资格；一轮统计里同一窗口只计一次，跨窗口均值按有效样本数加权合成。

迟报补到后的重算范围：

* 归并已经开工（``启动归并``）但尚未交接时，收到迟报更正只刷新当前轮次，
  不会另起一轮，也不会把同一窗口重复计入；
* 更正必须在提交（``确认归并``/开具交接）之前完成审核，未审核的迟报点会
  阻止交接；
* 每份交接都会固化当时实际采用的窗口集合、每窗有效样本数与窗均值，
  后续轮次或重复迟报都不会改写旧交接，旧交接可随时从原窗口复盘。
"""
from __future__ import annotations

import math
import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.downsample import (
    DEFAULT_MIN_VALID_RATIO,
    DownsampleService,
    _as_float,
    _as_int,
    sample_identity,
)
from app.store import store


class RollupWindowService(BaseModuleService):
    MODULE = "rollup_window"
    ENTITY = "归并窗口"
    OPERATION_TERM = "聚合"
    LIST_FIELDS = ["窗口编号", "测点编号", "窗口类型", "窗口长度", "滑动步长", "聚合指标", "窗口条数", "归并水位",
                   "有效样本数", "预期样本数", "分母", "合并值", "归并轮次", "交接编号"]
    REQUIRED_FIELDS = ["窗口编号", "测点编号", "窗口类型"]
    STATUS_ORDER = ["草稿", "待归并", "已归并", "已作废"]
    ACTION_RULES = {"启动归并": "待归并", "确认归并": "已归并", "标记作废": "已作废"}
    # 数据归并操作：按窗口归并规则执行聚合计算生成归并结果数据集
    NEGATIVE_ACTIONS = []

    # -- 状态持久化 -------------------------------------------------------

    def _state_file(self, entry: dict[str, Any]):
        return self._data_dir("rollup_windows") / f"{entry['窗口编号']}_state.json"

    def _rollup_file(self, entry: dict[str, Any]):
        return self._data_dir("rollup_windows") / f"{entry['窗口编号']}_rollup.jsonl"

    def _load_state(self, entry: dict[str, Any]) -> dict[str, Any]:
        state = self._read_json_file(self._state_file(entry))
        if not isinstance(state, dict):
            state = {}
        state.setdefault("rollup_id", entry["窗口编号"])
        state.setdefault("point", entry.get("测点编号", ""))
        state.setdefault("rounds", [])
        state.setdefault("handoffs", [])
        state.setdefault("reviews", [])
        return state

    def _save_state(self, entry: dict[str, Any], state: dict[str, Any]) -> None:
        self._write_json_file(self._state_file(entry), state)
        # 归并结果文件只落最近一轮：同一窗口在文件里只保留一行，
        # 历史轮次留在 state 里供复盘，避免索引/快照/导出把跨轮同窗重复统计。
        rounds = state.get("rounds", [])
        latest = rounds[-1] if rounds else None
        rows: list[dict[str, Any]] = []
        if latest is not None:
            for index, window in enumerate(latest.get("windows", []), start=1):
                rows.append({
                    "window_id": entry["窗口编号"],
                    "round": latest.get("round"),
                    "window_key": window.get("window_key"),
                    "source_task": window.get("task_id"),
                    "source_window_index": window.get("window_index"),
                    "point": window.get("point", entry.get("测点编号", "")),
                    "window_start": index,
                    "window_end": index,
                    "record_count": window.get("valid_count", 0),
                    "valid_count": window.get("valid_count", 0),
                    "expected_count": window.get("expected_count", 0),
                    "denominator": window.get("denominator", 0),
                    "rollup_value": window.get("mean", 0),
                    "mean_value": window.get("mean", 0),
                    "combined_value": latest.get("combined_value", 0),
                    "metric": latest.get("metric", "均值"),
                    "eligible": window.get("eligible", False),
                })
        self._write_jsonl(self._rollup_file(entry), rows, append=False)

    # -- 降采样窗口读取 ---------------------------------------------------

    def _downsample_files(self, point_key: str) -> list[Any]:
        """列出全部降采样产物文件。

        文件名按任务编号命名，不一定含测点编号，因此这里不做文件名过滤，
        交给 ``_load_windows`` 按记录里的 point 字段精确过滤。
        """
        ds_dir = self._data_dir("downsampled")
        if not ds_dir.exists():
            return []
        return sorted(ds_dir.glob("*_downsampled.jsonl"))

    @staticmethod
    def _window_identity(record: dict[str, Any]) -> str:
        key = record.get("window_key")
        if key:
            return str(key)
        task = record.get("task_id")
        index = record.get("window_index")
        if task is not None and index is not None:
            return f"{task}:{index}"
        return f"anon:{sample_identity(record)}"

    @staticmethod
    def _match_sample_id(window: dict[str, Any], sample_id: str) -> list[str]:
        """按原始 sample_id 或其去重标识匹配窗口内的迟报样本。"""
        target = str(sample_id)
        matches = []
        for identity in window.get("late_sample_ids") or []:
            identity = str(identity)
            if identity == target or identity.endswith(f":{target}"):
                matches.append(identity)
        return matches

    def _eligibility(self, record: dict[str, Any], valid: int, expected: int,
                     denominator: int) -> dict[str, Any]:
        min_valid = _as_int(record.get("min_valid"), 0)
        if min_valid <= 0:
            ratio = _as_float(record.get("最小有效比例"), DEFAULT_MIN_VALID_RATIO)
            if ratio <= 0 or ratio > 1:
                ratio = DEFAULT_MIN_VALID_RATIO
            expected_for_threshold = expected if expected > 0 else valid
            # 缺省阈值同降采样侧保持一致：预期 10 次至少 7 个有效点。
            min_valid = max(1, math.ceil(expected_for_threshold * ratio - 1e-9))
        reasons: list[str] = []
        if valid <= 0:
            reasons.append("没有有效样本")
        if expected <= 0:
            reasons.append("预期样本数为 0")
        if denominator != valid:
            reasons.append("计入均值的分母与有效样本数不一致")
        if valid < min_valid:
            reasons.append(f"有效样本 {valid} 未达阈值 {min_valid}")
        stored = record.get("eligibility")
        if isinstance(stored, dict) and stored.get("ineligible_reasons"):
            reasons.extend(str(item) for item in stored["ineligible_reasons"])
        # 去重原因，保持可读
        reasons = list(dict.fromkeys(reasons))
        return {
            "eligible": not reasons,
            "threshold_met": valid >= min_valid,
            "denominator_verified": denominator == valid,
            "min_valid": min_valid,
            "ineligible_reasons": reasons,
        }

    def _normalize_window(self, record: dict[str, Any]) -> dict[str, Any]:
        window_key = self._window_identity(record)
        task_id = str(record.get("task_id") or window_key.split(":")[0])
        window_index = _as_int(record.get("window_index"), 0)
        valid = record.get("valid_count")
        if valid is None:
            valid = record.get("source_count")
        if valid is None:
            valid = 1
        valid = max(0, _as_int(valid))
        denominator = record.get("denominator")
        if denominator is None:
            denominator = valid
        denominator = max(0, _as_int(denominator))
        expected = record.get("expected_count")
        if expected is None:
            expected = max(valid, _as_int(record.get("source_count"), valid))
        expected = max(0, _as_int(expected))
        eligibility = self._eligibility(record, valid, expected, denominator)
        mean = _as_float(record.get("aggregated_value"))
        if "aggregated_value" not in record and "mean" in record:
            mean = _as_float(record.get("mean"))
        return {
            "window_key": window_key,
            "task_id": task_id,
            "window_index": window_index,
            "point": record.get("point", ""),
            "valid_count": valid,
            "expected_count": expected,
            "denominator": denominator,
            "mean": round(mean, 6),
            "eligible": eligibility["eligible"],
            "threshold_met": eligibility["threshold_met"],
            "denominator_verified": eligibility["denominator_verified"],
            "min_valid": eligibility["min_valid"],
            "ineligible_reasons": eligibility["ineligible_reasons"],
            "late_sample_ids": list(record.get("late_sample_ids") or []),
            "late_sample_count": _as_int(record.get("late_sample_count"), 0),
            "source_ref": {
                "artifact": "downsampled",
                "window_key": window_key,
                "task_id": task_id,
                "window_index": window_index,
            },
        }

    def _load_windows(self, point_key: str, *, include_all_points: bool = False) -> list[dict[str, Any]]:
        """读取该测点全部降采样窗口，按窗口标识去重，同一窗口只保留一份。

        ``include_all_points=True`` 时用于解析调用方显式点名的窗口键，
        此时可以跨测点取窗，但同样按窗口标识去重。
        """
        seen: set[str] = set()
        windows: list[dict[str, Any]] = []
        for path in self._downsample_files(point_key):
            for record in self._read_jsonl(path):
                record_point = str(record.get("point") or "")
                if (not include_all_points and record_point and point_key
                        and record_point != point_key):
                    continue
                window = self._normalize_window(record)
                if window["window_key"] in seen:
                    continue
                seen.add(window["window_key"])
                windows.append(window)
        windows.sort(key=lambda item: (item["task_id"], item["window_index"]))
        return windows

    def _resolve_explicit_windows(self, point_key: str,
                                  keys: list[str]) -> list[dict[str, Any]]:
        """按窗口键取窗，允许跨测点；只返回真实存在的窗口。"""
        if not keys:
            return []
        catalog = {w["window_key"]: w for w in
                   self._load_windows(point_key, include_all_points=True)}
        return [dict(catalog[key]) for key in keys if key in catalog]

    # -- 合成口径 ---------------------------------------------------------

    def _combine(self, windows: list[dict[str, Any]], metric: str) -> tuple[float, int]:
        """按每个窗口的实际有效点数加权合成均值，返回 (合并值, 分母)。"""
        usable = [w for w in windows if w["eligible"] and w["denominator"] > 0]
        if not usable:
            return 0.0, 0
        if metric in ("最大值", "max"):
            return round(max(w["mean"] for w in usable), 4), sum(w["denominator"] for w in usable)
        if metric in ("最小值", "min"):
            return round(min(w["mean"] for w in usable), 4), sum(w["denominator"] for w in usable)
        if metric in ("求和", "sum"):
            return round(sum(w["mean"] for w in usable), 4), sum(w["denominator"] for w in usable)
        denominator = sum(w["denominator"] for w in usable)
        weighted = sum(w["mean"] * w["denominator"] for w in usable)
        return (round(weighted / denominator, 4) if denominator else 0.0), denominator

    def _union_windows(self, state: dict[str, Any], point_key: str,
                       current: dict[str, dict[str, Any]],
                       extra_keys: list[str] | None = None) -> list[dict[str, Any]]:
        """上一轮已采用的窗口 ∪ 本轮新增窗口，按窗口标识去重，刷新为当前计数。

        历史轮次采用过的窗口可能属于其他测点（例如下一轮并入的合格窗），
        这里按窗口键跨测点取回当前计数，保证新统计沿用同一集合。
        """
        order: list[str] = []
        for round_info in state.get("rounds", []):
            for window in round_info.get("windows", []):
                key = window.get("window_key")
                if key and key not in order:
                    order.append(key)
        for key in extra_keys or []:
            if key not in order:
                order.append(key)
        catalog = {w["window_key"]: w for w in
                   self._resolve_explicit_windows(point_key, order)}
        catalog.update(current)
        return [dict(catalog[key]) for key in order if key in catalog]

    # -- 审核门禁 ---------------------------------------------------------

    @classmethod
    def _pending_reviews(cls, state: dict[str, Any], windows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        approved = {
            (str(item.get("window_key")), str(item.get("sample_id")))
            for item in state.get("reviews", [])
            if item.get("status") == "已审核"
        }
        pending: list[dict[str, Any]] = []
        for window in windows:
            for sample_id in window.get("late_sample_ids") or []:
                matching = cls._match_sample_id(window, str(sample_id))
                if any((str(window.get("window_key")), sid) in approved for sid in matching):
                    continue
                pending.append({
                    "window_key": window.get("window_key"),
                    "sample_id": sample_id,
                    "valid_count": window.get("valid_count"),
                    "expected_count": window.get("expected_count"),
                    "threshold_met": window.get("threshold_met"),
                })
        return pending

    # -- 归并主流程 -------------------------------------------------------

    def rollup_windows(self, rollup_id: int, *, round_no: int | None = None,
                       include_window_keys: list[str] | None = None,
                       candidates: list[str] | None = None) -> dict[str, Any]:
        """按窗口归并规则对降采样后的时序数据执行聚合计算，生成归并结果数据集。

        默认行为：还没有轮次时建立第 1 轮；当前轮尚未交接时刷新当前轮
        （迟报补到的重算只影响这一轮）；已交接后默认开启下一轮。
        """
        entry = store.find(self.MODULE, rollup_id)
        if entry is None:
            return {"error": f"归并窗口 {rollup_id} 不存在"}
        if entry.get("status") == "已作废":
            return {"error": f"归并窗口 {entry['窗口编号']} 已作废，不能继续归并"}
        self.run_action(rollup_id, "启动归并")
        point_key = entry["测点编号"]
        metric = entry.get("聚合指标", "均值")
        extra_keys = list(include_window_keys or candidates or [])

        state = self._load_state(entry)
        current_map = {w["window_key"]: w for w in self._load_windows(point_key)}
        # 显式点名的窗口允许跨测点取窗（例如下一轮并入另一测点的合格窗），
        # 但仍按窗口标识去重，保证一轮里同一窗口只出现一次。
        for window in self._resolve_explicit_windows(point_key, extra_keys):
            current_map.setdefault(window["window_key"], window)

        rounds: list[dict[str, Any]] = state["rounds"]
        if round_no is None:
            if not rounds:
                round_no = 1
            elif state["handoffs"] and rounds[-1].get("round") == state["handoffs"][-1].get("round"):
                round_no = len(rounds) + 1
            else:
                round_no = _as_int(rounds[-1].get("round"), len(rounds))
        existing = next((r for r in rounds if _as_int(r.get("round")) == round_no), None)

        if existing is not None:
            # 刷新当前轮：沿用该轮已采用的窗口集合（可能含其他测点的合格窗），
            # 只把“因迟报更正刚达阈值”的窗口补进来，把重算范围限制在受影响窗口，
            # 而不是重新扫描所有合格窗。
            adopted_keys = [w.get("window_key") for w in existing.get("windows", [])]
            catalog = {w["window_key"]: w for w in
                       self._resolve_explicit_windows(point_key, adopted_keys)}
            catalog.update(current_map)
            adopted = [dict(catalog[key]) for key in adopted_keys if key in catalog]
            adopted_set = {w["window_key"] for w in adopted}
            for window in current_map.values():
                affected = bool(window.get("late_sample_ids"))
                if (window["eligible"] and affected
                        and window["window_key"] not in adopted_set):
                    adopted.append(dict(window))
                    adopted_set.add(window["window_key"])
            for key in extra_keys:
                if key in catalog and key not in adopted_set:
                    adopted.append(dict(catalog[key]))
                    adopted_set.add(key)
        else:
            if round_no == 1:
                adopted = [dict(w) for w in current_map.values() if w["eligible"]]
            else:
                adopted = self._union_windows(state, point_key, current_map, extra_keys)
            # 显式纳入的窗口仍要求当前合格
            for key in extra_keys:
                if key in current_map and key not in {w["window_key"] for w in adopted}:
                    adopted.append(dict(current_map[key]))

        # 去重，确保同一窗口在一轮统计里只出现一次
        deduped: list[dict[str, Any]] = []
        seen: set[str] = set()
        for window in adopted:
            if window["window_key"] in seen:
                continue
            seen.add(window["window_key"])
            deduped.append(window)
        # 只采用资格通过的窗口：计数与分母必须来自同一集合，
        # 不合格的窗口单独列出原因，不进入本轮统计。
        rejected = [w for w in deduped if not w["eligible"]]
        adopted = [w for w in deduped if w["eligible"]]

        combined_value, denominator = self._combine(adopted, metric)
        pending = self._pending_reviews(state, adopted)
        committed = not pending and bool(adopted)
        round_info = {
            "round": round_no,
            "windows": adopted,
            "denominator": denominator,
            "combined_value": combined_value,
            "metric": metric,
            "committed": committed,
            "pending_review_count": len(pending),
            "updated_at": time.time(),
        }
        if existing is not None:
            rounds[rounds.index(existing)] = round_info
        else:
            rounds.append(round_info)
        state["rounds"] = sorted(rounds, key=lambda item: _as_int(item.get("round")))

        if committed:
            self.run_action(rollup_id, "确认归并")
        entry["窗口条数"] = str(len(adopted))
        entry["归并水位"] = str(len(adopted))
        entry["有效样本数"] = str(sum(w["valid_count"] for w in adopted))
        entry["预期样本数"] = str(sum(w["expected_count"] for w in adopted))
        entry["分母"] = str(denominator)
        entry["合并值"] = str(combined_value)
        entry["归并轮次"] = str(round_no)
        self._save_state(entry, state)

        return {
            "rollup_id": entry["窗口编号"],
            "point": point_key,
            "window_type": entry.get("窗口类型", "固定窗口"),
            "round": round_no,
            "window_period_count": len(adopted),
            "windows": adopted,
            "window_keys": [w["window_key"] for w in adopted],
            "denominator": denominator,
            "combined_value": combined_value,
            "metric": metric,
            "committed": committed,
            "pending_reviews": pending,
            "rejected_windows": [
                {"window_key": w["window_key"], "valid_count": w["valid_count"],
                 "expected_count": w["expected_count"], "reasons": w["ineligible_reasons"]}
                for w in rejected
            ],
            "eligible_window_count": sum(1 for w in current_map.values() if w["eligible"]),
        }

    # -- 迟报补入与审核 ---------------------------------------------------

    def submit_late_correction(self, rollup_id: int, *, window_key: str | None = None,
                               task_id: int | str | None = None,
                               window_index: int | None = None,
                               sample: dict[str, Any] | None = None,
                               value: float | None = None,
                               sample_id: str | None = None) -> dict[str, Any]:
        """归并开工后收到迟报更正：把有效点补进原降采样窗口并登记待审核。

        更正只作用于原窗口，重算范围限定在当前未交接的轮次；重复提交同一迟报点
        由降采样侧幂等去重，不会抬高有效样本数与分母。
        """
        entry = store.find(self.MODULE, rollup_id)
        if entry is None:
            return {"error": f"归并窗口 {rollup_id} 不存在"}
        if window_key and (task_id is None or window_index is None):
            task_part, _, index_part = str(window_key).partition(":")
            task_id = task_id if task_id is not None else task_part
            window_index = window_index if window_index is not None else _as_int(index_part, 0)
        if task_id is None or window_index is None:
            return {"error": "迟报更正需要 window_key 或 task_id + window_index"}
        state = self._load_state(entry)
        handoff_rounds = {_as_int(h.get("round")) for h in state.get("handoffs", [])}
        current_round = _as_int(state["rounds"][-1].get("round")) if state.get("rounds") else 0
        result = DownsampleService().apply_late_sample(
            task_id, int(window_index), sample,
            value=value, sample_id=sample_id,
        )
        if "error" in result:
            return result
        result["rollup_id"] = entry["窗口编号"]
        result["review_status"] = "待审核"
        result["rollup_started"] = bool(current_round)
        # 已交接轮次不再改动；该更正计入即将发起的下一轮，旧交接保持原样。
        already_handed_off = bool(current_round) and current_round in handoff_rounds
        result["applies_to_round"] = current_round + 1 if already_handed_off else max(current_round, 1)
        result["supersedes_handoff"] = bool(already_handed_off and not result["deduplicated"])
        return result

    def review_late_sample(self, rollup_id: int, *, window_key: str | None = None,
                           sample_id: str | None = None,
                           approve: bool = True) -> dict[str, Any]:
        """提交前完成迟报点审核：核对该点确实计入原窗口、复核分母并确认窗口资格。

        审核只登记结论，不改写历史交接快照；旧交接任何时候都能从原窗口复盘。
        """
        entry = store.find(self.MODULE, rollup_id)
        if entry is None:
            return {"error": f"归并窗口 {rollup_id} 不存在"}
        state = self._load_state(entry)
        # 审核可能针对跨测点并入的窗口，按窗口键取窗时允许全量目录。
        windows = self._load_windows(entry["测点编号"], include_all_points=bool(window_key))
        window_map = {w["window_key"]: w for w in windows}

        targets: list[dict[str, Any]] = []
        if window_key:
            window = window_map.get(str(window_key))
            if window is None:
                return {"error": f"窗口 {window_key} 不存在或尚无降采样结果"}
            targets = [window]
        elif sample_id:
            targets = [w for w in windows if self._match_sample_id(w, sample_id)]
            if not targets:
                return {"error": f"没有窗口记录迟报点 {sample_id}"}
        else:
            targets = [w for w in windows if w.get("late_sample_ids")]

        reviewed: list[dict[str, Any]] = []
        for window in targets:
            ids = [str(item) for item in (window.get("late_sample_ids") or [])]
            if sample_id:
                selected = [sid for sid in ids if self._match_sample_id(window, sample_id)]
            else:
                selected = ids
            for sid in selected:
                review = {
                    "window_key": window["window_key"],
                    "sample_id": sid,
                    "status": "已审核" if approve else "已驳回",
                    "valid_count": window["valid_count"],
                    "expected_count": window["expected_count"],
                    "denominator": window["denominator"],
                    "threshold_met": window["threshold_met"],
                    "eligible": window["eligible"],
                    "reviewed_at": time.time(),
                }
                state["reviews"].append(review)
                reviewed.append(review)
                if not approve:
                    # 驳回的迟报点必须从窗口计数里撤回，否则它会继续抬高分母，
                    # 甚至把窗口误判为合格。
                    revert = DownsampleService().revert_late_sample(
                        window["task_id"], window["window_index"], sid)
                    review["reverted"] = "error" not in revert
                    review["after_revert"] = {
                        key: revert.get(key) for key in
                        ("valid_count", "denominator", "eligible", "threshold_met")
                    } if "error" not in revert else {"error": revert["error"]}
        if not reviewed:
            return {"error": "没有待审核的迟报点"}
        self._save_state(entry, state)
        return {
            "rollup_id": entry["窗口编号"],
            "reviewed_count": len(reviewed),
            "reviews": reviewed,
            "approved": approve,
        }

    # -- 交接 -------------------------------------------------------------

    def create_handoff(self, rollup_id: int, *, round_no: int | None = None) -> dict[str, Any]:
        """用首次归并结果开具交接，固化当时实际采用的窗口集合与每窗有效点数。"""
        entry = store.find(self.MODULE, rollup_id)
        if entry is None:
            return {"error": f"归并窗口 {rollup_id} 不存在"}
        state = self._load_state(entry)
        rounds = state.get("rounds", [])
        if not rounds:
            return {"error": "还没有归并结果，无法交接"}
        if round_no is None:
            round_no = _as_int(rounds[-1].get("round"))
        round_info = next((r for r in rounds if _as_int(r.get("round")) == round_no), None)
        if round_info is None:
            return {"error": f"归并窗口 {entry['窗口编号']} 不存在第 {round_no} 轮"}
        # 该轮还没交接：先按原窗口集合刷新一次，让刚补到的迟报点进入审核门禁，
        # 避免交接用上过期计数。
        already_handed_off = any(
            _as_int(h.get("round")) == round_no for h in state.get("handoffs", []))
        if not already_handed_off:
            self.rollup_windows(rollup_id, round_no=round_no)
            state = self._load_state(entry)
            round_info = next(
                (r for r in state.get("rounds", []) if _as_int(r.get("round")) == round_no),
                round_info)
        if not round_info.get("windows"):
            return {"error": f"第 {round_no} 轮没有可交接的合格窗口"}

        pending = self._pending_reviews(state, round_info["windows"])
        if pending:
            return {"error": "仍有迟报点未审核，提交前需完成审核",
                    "pending_reviews": pending}

        existing = next(
            (h for h in state.get("handoffs", []) if _as_int(h.get("round")) == round_no),
            None,
        )
        if existing is not None:
            return {**existing, "reused": True}

        handoff_id = f"HANDOFF-{entry['窗口编号']}-R{round_no}-{len(state.get('handoffs', [])) + 1}"
        handoff = {
            "handoff_id": handoff_id,
            "rollup_id": entry["窗口编号"],
            "point": entry.get("测点编号", ""),
            "round": round_no,
            "metric": round_info.get("metric", entry.get("聚合指标", "均值")),
            "windows": [
                {
                    "window_key": w["window_key"],
                    "task_id": w["task_id"],
                    "window_index": w["window_index"],
                    "valid_count": w["valid_count"],
                    "expected_count": w["expected_count"],
                    "denominator": w["denominator"],
                    "mean": w["mean"],
                    "source_ref": w.get("source_ref", {}),
                }
                for w in round_info["windows"]
            ],
            "window_keys": [w["window_key"] for w in round_info["windows"]],
            "denominator": round_info.get("denominator", 0),
            "combined_value": round_info.get("combined_value", 0),
            "created_at": time.time(),
        }
        state["handoffs"].append(handoff)
        self._save_state(entry, state)
        self._write_json_file(
            self._data_dir("rollup_windows") / f"{handoff_id}.json", handoff,
        )
        entry["交接编号"] = handoff_id
        self.run_action(rollup_id, "确认归并")
        return {**handoff, "reused": False}

    def replay_handoff(self, rollup_id: int, handoff_id: str | None = None) -> dict[str, Any]:
        """从旧交接还原当时所用的窗口集合与合成口径，后续轮次不会改写它。"""
        entry = store.find(self.MODULE, rollup_id)
        if entry is None:
            return {"error": f"归并窗口 {rollup_id} 不存在"}
        state = self._load_state(entry)
        handoffs = state.get("handoffs", [])
        if not handoffs:
            return {"error": f"归并窗口 {entry['窗口编号']} 还没有交接记录"}
        handoff = None
        if handoff_id:
            handoff = next((h for h in handoffs if h.get("handoff_id") == handoff_id), None)
        else:
            handoff = handoffs[-1]
        if handoff is None:
            return {"error": f"交接 {handoff_id} 不存在"}
        current = {w["window_key"]: w for w in self._load_windows(entry["测点编号"])}
        windows = []
        for window in handoff.get("windows", []):
            snapshot = dict(window)
            live = current.get(window.get("window_key"))
            snapshot["replayable"] = live is not None
            snapshot["current_valid_count"] = live["valid_count"] if live else None
            windows.append(snapshot)
        denominator = sum(_as_int(w.get("denominator")) for w in windows)
        combined = 0.0
        if denominator:
            combined = round(
                sum(_as_float(w.get("mean")) * _as_int(w.get("denominator")) for w in windows)
                / denominator, 4)
        return {
            "handoff_id": handoff.get("handoff_id"),
            "rollup_id": entry["窗口编号"],
            "point": handoff.get("point"),
            "round": handoff.get("round"),
            "metric": handoff.get("metric"),
            "windows": windows,
            "window_keys": handoff.get("window_keys", []),
            "denominator": denominator,
            "combined_value": handoff.get("combined_value", combined),
            "replayed_value": combined,
            "created_at": handoff.get("created_at"),
            "restorable": all(w["replayable"] for w in windows),
        }

    def next_round(self, rollup_id: int, *, include_window_keys: list[str] | None = None,
                   candidates: list[str] | None = None) -> dict[str, Any]:
        """紧接着由交接人员发起下一轮：沿用已采用窗口并纳入另一合格窗，按有效点数合成。"""
        entry = store.find(self.MODULE, rollup_id)
        if entry is None:
            return {"error": f"归并窗口 {rollup_id} 不存在"}
        state = self._load_state(entry)
        rounds = state.get("rounds", [])
        if not rounds:
            return self.rollup_windows(rollup_id, include_window_keys=include_window_keys)

        # 第 1 轮先把窗口集合和每窗有效点数交接出去；后续轮次若要启用，
        # 必须先落在一次已确认的交接上，避免“开工了但没交接”的状态被跳过。
        if not state.get("handoffs"):
            self.rollup_windows(rollup_id)
            first = self.create_handoff(rollup_id)
            if "error" in first:
                return first
            state = self._load_state(entry)
            rounds = state.get("rounds", [])

        next_no = _as_int(rounds[-1].get("round")) + 1
        return self.rollup_windows(
            rollup_id, round_no=next_no,
            include_window_keys=list(include_window_keys or candidates or []),
        )

    def confirm_rollup(self, rollup_id: int) -> dict[str, Any]:
        """提交归并：迟报点全部审核通过后才允许确认。"""
        entry = store.find(self.MODULE, rollup_id)
        if entry is None:
            return {"error": f"归并窗口 {rollup_id} 不存在"}
        state = self._load_state(entry)
        if not state.get("rounds"):
            return {"error": "还没有归并结果"}
        round_info = state["rounds"][-1]
        pending = self._pending_reviews(state, round_info.get("windows", []))
        if pending:
            return {"error": "仍有迟报点未审核，提交前需完成审核",
                    "pending_reviews": pending}
        if round_info.get("windows"):
            self.run_action(rollup_id, "确认归并")
        return {
            "rollup_id": entry["窗口编号"],
            "round": round_info.get("round"),
            "committed": bool(round_info.get("windows")),
            "denominator": round_info.get("denominator"),
            "combined_value": round_info.get("combined_value"),
        }

    def rollup_summary(self, rollup_id: int) -> dict[str, Any]:
        """汇总当前轮次、交接与待审核更正，供页面和跨模块消费。"""
        entry = store.find(self.MODULE, rollup_id)
        if entry is None:
            return {"error": f"归并窗口 {rollup_id} 不存在"}
        state = self._load_state(entry)
        latest = state["rounds"][-1] if state.get("rounds") else None
        return {
            "rollup_id": entry["窗口编号"],
            "round": latest.get("round") if latest else None,
            "windows": latest.get("windows", []) if latest else [],
            "denominator": latest.get("denominator", 0) if latest else 0,
            "combined_value": latest.get("combined_value", 0) if latest else 0,
            "handoffs": state.get("handoffs", []),
            "pending_reviews": self._pending_reviews(state, latest.get("windows", [])) if latest else [],
        }

    def compute_cumulative_rollup(self, rollup_id: int) -> dict[str, Any]:
        """对最近一轮归并窗口计算累计聚合指标写入累计快照数据文件。

        累计口径同样按窗口去重，再按有效样本数加权，避免同一窗口被重复计入。
        """
        entry = store.find(self.MODULE, rollup_id)
        if entry is None:
            return {"error": f"归并窗口 {rollup_id} 不存在"}
        rollup_result = self.rollup_windows(rollup_id)
        if "error" in rollup_result:
            return rollup_result
        state = self._load_state(entry)
        latest = state["rounds"][-1] if state.get("rounds") else None
        windows = latest.get("windows", []) if latest else []
        cumulative = []
        running_sum = 0.0
        running_denominator = 0
        for i, window in enumerate(windows):
            valid = _as_int(window.get("valid_count"))
            denominator = _as_int(window.get("denominator"), valid)
            val = _as_float(window.get("mean"))
            running_sum += val * denominator
            running_denominator += denominator
            cumulative.append({
                "window_index": i + 1,
                "point": window.get("point", entry.get("测点编号", "")),
                "window_key": window.get("window_key"),
                "window_value": val,
                "valid_count": valid,
                "denominator": denominator,
                "cumulative_value": round(
                    running_sum / running_denominator, 4) if running_denominator else 0.0,
                "window_id": entry["窗口编号"],
                "round": latest.get("round") if latest else 0,
            })
        # 写入累计聚合数据文件
        cum_file = self._data_dir("rollup_windows") / f"{entry['窗口编号']}_cumulative.jsonl"
        self._write_jsonl(cum_file, cumulative, append=False)
        final_value = cumulative[-1]["cumulative_value"] if cumulative else 0.0
        return {
            "rollup_id": entry["窗口编号"],
            "round": latest.get("round") if latest else 0,
            "cumulative_periods": len(cumulative),
            "denominator": running_denominator,
            "final_cumulative_value": final_value,
        }

    # -- 语义别名：保持调用方可读性 ---------------------------------------

    submit_late_sample = submit_late_correction
    apply_late_correction = submit_late_correction
    approve_late_sample = review_late_sample
    audit_late_point = review_late_sample
    issue_handoff = create_handoff
    open_handoff = create_handoff
    handoff = create_handoff
    replay = replay_handoff
    restore_handoff = replay_handoff
    verify_handoff = replay_handoff
    rollup_next_round = next_round
    run_next_round = next_round
    start_next_round = next_round
