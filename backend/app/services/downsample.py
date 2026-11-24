"""降采样领域服务：维护高频时序到目标频率的降采样聚合方式与压缩比产出。

按聚合方式对时序数据进行降采样计算，写入降采样结果序列数据集。
每个降采样窗口除聚合值外，还携带窗口资格所需的三个计数：

* ``expected_count``（预期样本数）：按源/目标频率推算该窗口应有的采样数；
* ``valid_count``（有效样本数）：实际收到并保留在窗口内的样本数（已去重、已剔除异常点）；
* ``denominator``（计入均值的分母）：真正参与均值计算的样本数。

归并阶段据此核实窗口资格，并按 ``denominator`` 作为权重合成跨窗口均值，
避免把预期样本数当成实际分母，或在同一次统计里把同一窗口计入两次。
"""
from __future__ import annotations

import json
import math
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 窗口资格缺省阈值：有效样本数至少达到预期样本数的该比例。
# 预期 10 条、比例 0.7 → 窗口至少要 7 个有效点才合格。
DEFAULT_MIN_VALID_RATIO = 0.7


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


def sample_identity(record: dict[str, Any]) -> str:
    """样本去重键：优先用采集时刻/样本号，没有稳定标识时退回整条记录内容。

    迟报点被重复提交时，只要携带同一采集时刻或样本号，就会落到同一个键上，
    从而只计一次有效样本，分母不会被重复抬高。
    """
    for key in ("sample_id", "sample_key", "seq", "timestamp", "时间", "采样时刻"):
        value = record.get(key)
        if value not in (None, ""):
            return f"{key}:{value}"
    return "json:" + json.dumps(record, ensure_ascii=False, sort_keys=True, default=str)


def _sample_value(record: dict[str, Any]) -> float:
    for key in ("calibrated_value_V", "raw_value_mV", "raw_value", "value", "aggregated_value"):
        if key in record and record[key] not in (None, ""):
            return _as_float(record[key])
    return 0.0


def _dedupe(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """按样本标识去重，保留首次出现的顺序。"""
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    for record in records:
        key = sample_identity(record)
        if key in seen:
            continue
        seen.add(key)
        result.append(record)
    return result


def _sort_by_time(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """有采集时刻时按时间稳定排序，便于迟报点落回原窗口。"""
    if records and all(str(r.get("timestamp") or r.get("时间") or "").strip() for r in records):
        return sorted(records, key=lambda r: str(r.get("timestamp") or r.get("时间")))
    return records


class DownsampleService(BaseModuleService):
    MODULE = "downsample"
    ENTITY = "降采样任务"
    OPERATION_TERM = "聚合"
    LIST_FIELDS = ["任务编号", "源序列", "源采样频率", "目标频率", "聚合方式", "输出点数", "压缩比", "任务水位"]
    REQUIRED_FIELDS = ["任务编号", "源序列", "源采样频率"]
    STATUS_ORDER = ["草稿", "待执行", "执行中", "已完成"]
    ACTION_RULES = {"启动执行": "执行中", "标记完成": "已完成", "标记失败": "已完成"}
    NEGATIVE_ACTIONS = []

    # -- 计数口径 ---------------------------------------------------------

    def _window_size(self, entry: dict[str, Any]) -> int:
        source_freq = _as_float(str(entry.get("源采样频率", "10")).replace("Hz", ""), 10.0)
        target_freq = _as_float(str(entry.get("目标频率", "1")).replace("Hz", ""), 1.0)
        if source_freq <= 0 or target_freq <= 0:
            return 1
        return max(1, int(source_freq / target_freq))

    def _expected_count(self, entry: dict[str, Any], window_size: int) -> int:
        declared = _as_int(entry.get("预期样本数"), 0)
        return declared if declared > 0 else window_size

    def _min_valid(self, entry: dict[str, Any], expected_count: int) -> int:
        declared = _as_int(entry.get("最低有效样本数"), 0)
        if declared > 0:
            return declared
        ratio = _as_float(entry.get("最小有效比例"), DEFAULT_MIN_VALID_RATIO)
        if ratio <= 0 or ratio > 1:
            ratio = DEFAULT_MIN_VALID_RATIO
        # 减去极小量，避免 10*0.7 这类浮点误差把阈值抬成 8。
        return max(1, math.ceil(expected_count * ratio - 1e-9))

    def _eligibility(self, valid_count: int, expected_count: int,
                     denominator: int, min_valid: int) -> dict[str, Any]:
        reasons: list[str] = []
        if expected_count <= 0:
            reasons.append("预期样本数为 0")
        if denominator != valid_count:
            reasons.append("计入均值的分母与有效样本数不一致")
        if valid_count <= 0:
            reasons.append("没有有效样本")
        if valid_count < min_valid:
            reasons.append(f"有效样本 {valid_count} 未达阈值 {min_valid}")
        return {
            "eligible": not reasons,
            "threshold_met": valid_count >= min_valid,
            "denominator_verified": denominator == valid_count,
            "min_valid": min_valid,
            "ineligible_reasons": reasons,
        }

    def _resolve_task(self, task_id: int | str) -> tuple[dict[str, Any] | None, str | None]:
        """既接受数据库 id，也接受任务编号，方便重算时直接按编号定位。"""
        if isinstance(task_id, int) or str(task_id).isdigit():
            entry = store.find(self.MODULE, int(task_id))
            if entry is not None:
                return entry, None
        for row in store.rows(self.MODULE):
            if str(row.get("任务编号")) == str(task_id):
                return row, None
        return None, f"降采样任务 {task_id} 不存在"

    def _output_file(self, entry: dict[str, Any]):
        return self._data_dir("downsampled") / f"{entry['任务编号']}_downsampled.jsonl"

    # -- 降采样主流程 -----------------------------------------------------

    # 数据聚合操作：按窗口执行降采样聚合计算生成降采样结果序列数据集
    def downsample_series(self, task_id: int, source_data_file: str | None = None) -> dict[str, Any]:
        """按聚合方式对高频时序数据进行降采样计算生成降采样结果数据集。

        窗口内同时统计预期、实收、剔除和有效样本数；聚合均值只除以有效样本数，
        并把该分母写进结果，供归并阶段校验窗口资格和加权合成。
        """
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"error": f"降采样任务 {task_id} 不存在"}
        entry, _ = self.run_action(task_id, "启动执行")
        point_key = entry.get("源序列", entry["任务编号"])
        agg_method = entry.get("聚合方式", "均值")

        # 有效样本来源：优先使用剔除异常后的序列
        filtered_file = self._data_dir("filtered_series") / f"{point_key}_filtered.jsonl"
        cal_file = self._data_dir("calibrated") / f"{point_key}_calibrated.jsonl"
        edge_file = self._data_dir("edge_samples") / f"{point_key}.jsonl"

        valid_records = self._read_jsonl(filtered_file)
        if not valid_records:
            valid_records = self._read_jsonl(cal_file)
        if not valid_records:
            valid_records = self._read_jsonl(edge_file)
        valid_records = _sort_by_time(_dedupe(valid_records))

        # 实收样本来源：优先使用剔除前的标定序列，用来区分“实收”和“有效”。
        received_records = self._read_jsonl(cal_file)
        if not received_records:
            received_records = self._read_jsonl(edge_file)
        received_records = _sort_by_time(_dedupe(received_records))

        # 迟报点可能只补进了有效序列而没进标定文件，也要计入实收。
        received_keys = {sample_identity(r) for r in received_records}
        for record in valid_records:
            key = sample_identity(record)
            if key not in received_keys:
                received_records.append(record)
                received_keys.add(key)
        received_records = _sort_by_time(received_records)

        window_size = self._window_size(entry)
        expected_count = self._expected_count(entry, window_size)
        min_valid = self._min_valid(entry, expected_count)
        valid_by_key = {sample_identity(r): r for r in valid_records}

        downsampled: list[dict[str, Any]] = []
        total_received = 0
        total_valid = 0
        total_excluded = 0
        for index in range(0, len(received_records), window_size):
            window = received_records[index:index + window_size]
            keys = [sample_identity(r) for r in window]
            valid_in_window = [key for key in keys if key in valid_by_key]
            values = [_sample_value(valid_by_key[key]) for key in valid_in_window]
            received_count = len(window)
            valid_count = len(valid_in_window)
            excluded_count = received_count - valid_count
            denominator = valid_count

            if agg_method in ("最大值", "max"):
                agg_val = round(max(values), 4) if values else 0.0
            elif agg_method in ("最小值", "min"):
                agg_val = round(min(values), 4) if values else 0.0
            elif agg_method in ("求和", "sum"):
                agg_val = round(sum(values), 4)
            else:
                agg_val = round(sum(values) / denominator, 4) if denominator else 0.0

            total_received += received_count
            total_valid += valid_count
            total_excluded += excluded_count
            window_index = len(downsampled) + 1
            window_key = f"{entry['任务编号']}:{window_index}"
            record = {
                "task_id": entry["任务编号"],
                "point": point_key,
                "window_index": window_index,
                "window_key": window_key,
                "source_count": received_count,
                "received_count": received_count,
                "excluded_count": excluded_count,
                "valid_count": valid_count,
                "expected_count": expected_count,
                "denominator": denominator,
                "threshold_met": valid_count >= min_valid,
                "min_valid": min_valid,
                "eligibility": self._eligibility(valid_count, expected_count, denominator, min_valid),
                "aggregated_value": agg_val,
                "value_sum": round(sum(values), 6),
                "sample_ids": valid_in_window,
                "late_sample_count": 0,
                "method": agg_method,
                "target_freq": _as_float(str(entry.get("目标频率", "1")).replace("Hz", ""), 1.0),
            }
            record["eligible"] = record["eligibility"]["eligible"]
            downsampled.append(record)

        # 将降采样结果写入降采样序列数据文件（整文件覆盖，重算不叠加旧窗口）
        output_file = self._output_file(entry)
        written = self._write_jsonl(output_file, downsampled, append=False)
        compression_ratio = round(len(valid_records) / max(written, 1), 2)
        entry["输出点数"] = str(written)
        entry["压缩比"] = str(compression_ratio)
        entry["任务水位"] = str(written)
        self.run_action(task_id, "标记完成")
        return {
            "task_id": entry["任务编号"],
            "point": point_key,
            "input_records": len(valid_records),
            "received_records": len(received_records),
            "excluded_records": total_excluded,
            "valid_records": total_valid,
            "output_points": written,
            "compression_ratio": compression_ratio,
            "method": agg_method,
            "expected_count": expected_count,
            "denominator": total_valid,
        }

    # -- 迟报点补入 -------------------------------------------------------

    def apply_late_sample(self, task_id: int | str, window_index: int,
                          sample: dict[str, Any] | None = None, *,
                          value: float | None = None,
                          sample_id: str | None = None) -> dict[str, Any]:
        """把一条迟报有效样本补进指定的降采样窗口，并重算该窗口的计数与均值。

        幂等：同一 ``sample_id``（缺省用采集时刻）重复提交时只返回现有计数，
        不会再次抬高 ``valid_count`` / ``denominator``。
        窗口在补入后可能“刚刚”达到阈值，届时 ``eligible`` 从 False 翻成 True，
        归并阶段即可采用该窗口。
        """
        entry, error = self._resolve_task(task_id)
        if entry is None:
            return {"error": error}
        output_file = self._output_file(entry)
        records = self._read_jsonl(output_file)
        if not records:
            return {"error": f"降采样任务 {entry['任务编号']} 还没有降采样结果"}

        record = next(
            (r for r in records if int(r.get("window_index", -1)) == int(window_index)),
            None,
        )
        if record is None:
            return {"error": f"{entry['任务编号']} 不存在窗口 {window_index}"}

        payload: dict[str, Any] = dict(sample or {})
        if sample_id is not None:
            payload.setdefault("sample_id", sample_id)
        if value is not None:
            payload["value"] = value
        if not payload:
            return {"error": "迟报样本缺少标识或数值"}

        identity = sample_identity(payload)
        sample_ids = list(record.get("sample_ids") or [])
        if identity in sample_ids:
            eligibility = self._eligibility(
                _as_int(record.get("valid_count")), _as_int(record.get("expected_count")),
                _as_int(record.get("denominator")), _as_int(record.get("min_valid"), 1))
            return {
                "task_id": entry["任务编号"],
                "window_index": int(window_index),
                "window_key": record.get("window_key"),
                "deduplicated": True,
                "valid_count": _as_int(record.get("valid_count")),
                "expected_count": _as_int(record.get("expected_count")),
                "denominator": _as_int(record.get("denominator")),
                "aggregated_value": _as_float(record.get("aggregated_value")),
                "threshold_met": eligibility["threshold_met"],
                "eligible": eligibility["eligible"],
                "eligibility": eligibility,
                "sample_id": identity,
            }

        sample_value = value if value is not None else _sample_value(payload)
        old_valid = _as_int(record.get("valid_count"))
        old_denominator = _as_int(record.get("denominator"), old_valid)
        old_sum = _as_float(record.get("value_sum"))
        if old_sum == 0 and old_valid:
            old_sum = _as_float(record.get("aggregated_value")) * old_denominator

        valid_count = old_valid + 1
        denominator = valid_count
        value_sum = old_sum + sample_value
        expected_count = _as_int(record.get("expected_count"), valid_count)
        min_valid = _as_int(record.get("min_valid") or 0) or self._min_valid(entry, expected_count)
        eligibility = self._eligibility(valid_count, expected_count, denominator, min_valid)
        late_sample_ids = list(record.get("late_sample_ids") or [])
        late_sample_ids.append(identity)
        late_samples = list(record.get("late_samples") or [])
        late_samples.append({"sample_id": identity, "value": sample_value})

        record.update({
            "valid_count": valid_count,
            "denominator": denominator,
            "expected_count": expected_count,
            "received_count": _as_int(record.get("received_count"), old_valid) + 1,
            "source_count": _as_int(record.get("source_count"), old_valid) + 1,
            "value_sum": round(value_sum, 6),
            "aggregated_value": round(value_sum / denominator, 4),
            "sample_ids": sample_ids + [identity],
            "late_sample_ids": late_sample_ids,
            "late_samples": late_samples,
            "late_sample_count": _as_int(record.get("late_sample_count")) + 1,
            "threshold_met": eligibility["threshold_met"],
            "eligible": eligibility["eligible"],
            "eligibility": eligibility,
            "min_valid": min_valid,
        })
        # 重写整份降采样结果，避免重复补入在文件里留下重复窗口
        self._write_jsonl(output_file, records, append=False)
        entry["输出点数"] = str(len(records))
        entry["任务水位"] = str(len(records))
        return {
            "task_id": entry["任务编号"],
            "point": record.get("point"),
            "window_index": int(window_index),
            "window_key": record.get("window_key") or f"{entry['任务编号']}:{window_index}",
            "deduplicated": False,
            "valid_count": valid_count,
            "expected_count": expected_count,
            "denominator": denominator,
            "received_count": record["received_count"],
            "aggregated_value": record["aggregated_value"],
            "threshold_met": eligibility["threshold_met"],
            "eligible": eligibility["eligible"],
            "eligibility": eligibility,
            "sample_id": identity,
            "value": sample_value,
        }

    def revert_late_sample(self, task_id: int | str, window_index: int,
                           sample_id: str) -> dict[str, Any]:
        """审核驳回时撤回一条迟报样本，恢复有效样本数、分母与窗口均值。

        只撤回来自已记录在案的迟报样本；原窗口自带的样本无法撤回。
        撤回后若窗口重新跌回阈值以下，``eligible`` 会变回 False，
        下一轮归并就不会再采用它。
        """
        entry, error = self._resolve_task(task_id)
        if entry is None:
            return {"error": error}
        output_file = self._output_file(entry)
        records = self._read_jsonl(output_file)
        record = next(
            (r for r in records if int(r.get("window_index", -1)) == int(window_index)),
            None,
        )
        if record is None:
            return {"error": f"{entry['任务编号']} 不存在窗口 {window_index}"}

        matches = [str(item) for item in (record.get("late_sample_ids") or [])
                   if str(item) == str(sample_id) or str(item).endswith(f":{sample_id}")]
        if not matches:
            return {"error": f"窗口里没有迟报样本 {sample_id}"}
        identity = matches[0]
        late_samples = list(record.get("late_samples") or [])
        reverted = next(
            (item for item in late_samples if item.get("sample_id") == identity), None)
        if reverted is None:
            return {"error": f"迟报样本 {sample_id} 缺少数值，无法撤回"}

        valid_count = max(0, _as_int(record.get("valid_count")) - 1)
        denominator = valid_count
        value_sum = _as_float(record.get("value_sum")) - _as_float(reverted.get("value"))
        expected_count = _as_int(record.get("expected_count"), valid_count)
        min_valid = _as_int(record.get("min_valid") or 0) or self._min_valid(entry, expected_count)
        eligibility = self._eligibility(valid_count, expected_count, denominator, min_valid)

        record.update({
            "valid_count": valid_count,
            "denominator": denominator,
            "received_count": max(0, _as_int(record.get("received_count")) - 1),
            "source_count": max(0, _as_int(record.get("source_count")) - 1),
            "value_sum": round(value_sum, 6),
            "aggregated_value": round(value_sum / denominator, 4) if denominator else 0.0,
            "sample_ids": [s for s in (record.get("sample_ids") or []) if s != identity],
            "late_sample_ids": [s for s in (record.get("late_sample_ids") or [])
                                if s != identity],
            "late_samples": [s for s in late_samples if s.get("sample_id") != identity],
            "late_sample_count": max(0, _as_int(record.get("late_sample_count")) - 1),
            "threshold_met": eligibility["threshold_met"],
            "eligible": eligibility["eligible"],
            "eligibility": eligibility,
            "min_valid": min_valid,
        })
        self._write_jsonl(output_file, records, append=False)
        return {
            "task_id": entry["任务编号"],
            "window_index": int(window_index),
            "window_key": record.get("window_key"),
            "reverted": True,
            "valid_count": valid_count,
            "expected_count": expected_count,
            "denominator": denominator,
            "aggregated_value": record["aggregated_value"],
            "eligible": eligibility["eligible"],
            "threshold_met": eligibility["threshold_met"],
            "sample_id": identity,
        }

    def batch_downsample(self, point_keys: list[str], agg_method: str = "均值") -> dict[str, Any]:
        """批量对多个测点的时序数据执行降采样聚合转换。"""
        results = []
        for pk in point_keys:
            tid = max((int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0) + 1
            self._auto_create_seed(self.MODULE, {
                "任务编号": f"DOWN-{pk}-{tid}",
                "源序列": pk,
                "源采样频率": "10Hz",
                "目标频率": "1Hz",
                "聚合方式": agg_method,
                "输出点数": "0",
                "压缩比": "0",
                "任务水位": "0",
            })
            r = self.downsample_series(tid)
            results.append(r)
        return {"total_points": len(point_keys), "executed": len(results), "results": results}
