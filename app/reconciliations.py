"""批量对账（接收方对同一移交批次的逐份签收对账）。

规则（与需求一致）：
  * 同一车次批量移交到达接收方后，接收方以【批次号 + 全局唯一签收单号 receipt_no +
    签收人 receiver + 统一签收时间 occurred_at】逐份提交覆盖原批次全部样本的对账清单；
  * 收妥项（kind=received）填写唯一事件号 event_id 与实核封签号 actual_seal_no；
    异常项（kind=exception）填写原因 reason 且不生成签收事件；
  * 收妥项须对应仍有效且处于该笔移交在途状态的事件，签收人/封签/时间满足现有单份
    签收规则（匹配在途移交单号、签收人=接收人、封签号与登记一致、签收晚于移交）；
  * 成功时各收妥项作为既有 receive 事件与各样本新版本在同一事务提交，
    异常项仅留对账记录；
  * 批次不存在、缺项、重复或外来项、事件号占用、任一收妥项不合格
    → 整单拒绝（事务回滚，不留任何对账/事件/版本记录，失败不占用签收单号）；
  * 一个批次只能成功对账一次；同单号同内容重试返回原结果（200 幂等），
    同单号异内容（含跨批次复用）→ 409 reconciliation_conflict；
  * 写事务以 BEGIN IMMEDIATE 串行，并发按已提交状态判定（唯一键兜底）；
  * 查询同时呈现当时收妥/异常明细及纠错后各收妥事件的现时有效性（event_effective /
    current_event_id / 当前 evaluation），不改写原对账。
"""
import json
import sqlite3
from typing import Any

from .db import utcnow_iso
from .schemas import ReconciliationIn
from .service import (
    DomainError,
    _batch_item_summary,
    _effective_event_id,
    _event_canonical,
    _fingerprint,
    evaluate,
    to_utc_iso,
)
from .versions import CHANGE_EVENT, record_version

KIND_RECEIVED = "received"
KIND_EXCEPTION = "exception"


# --------------------------------------------------------------------------- 指纹与读取

def _request_fingerprint(batch_id: str, data: ReconciliationIn) -> tuple[str, str]:
    """返回 (规范后的统一签收时间 UTC ISO, 请求指纹)。

    指纹对 items 提交顺序不敏感（按样本编号归一化排序）；纳入批次号，
    使同一签收单号跨批次复用必然表现为异内容冲突。
    批内缺项/重复/外来项属于请求不合法，由提交函数另行判定。
    """
    occurred = to_utc_iso(data.occurred_at)
    items = sorted(
        (
            {
                "sample_code": it.sample_code,
                "kind": it.kind,
                "event_id": it.event_id,
                "actual_seal_no": it.actual_seal_no,
                "reason": it.reason.strip() if it.reason else None,
            }
            for it in data.items
        ),
        key=lambda x: x["sample_code"],
    )
    canonical = {
        "batch_id": batch_id,
        "receipt_no": data.receipt_no,
        "receiver": data.receiver,
        "occurred_at": occurred,
        "items": items,
    }
    return occurred, _fingerprint(canonical)


def _get_batch(conn: sqlite3.Connection, batch_id: str) -> sqlite3.Row:
    batch = conn.execute(
        "SELECT * FROM transfer_batch WHERE batch_id = ?", (batch_id,)
    ).fetchone()
    if batch is None:
        raise DomainError(404, "batch_not_found", f"批次不存在: {batch_id}")
    return batch


def _batch_items(conn: sqlite3.Connection, batch_pk: int) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM transfer_batch_item WHERE batch_pk = ? ORDER BY item_index ASC",
        (batch_pk,),
    ).fetchall()


def _get_reconciliation_by_receipt(conn: sqlite3.Connection, receipt_no: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM batch_reconciliation WHERE receipt_no = ?", (receipt_no,)
    ).fetchone()


def _get_reconciliation_by_batch(conn: sqlite3.Connection, batch_pk: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM batch_reconciliation WHERE batch_pk = ?", (batch_pk,)
    ).fetchone()


# --------------------------------------------------------------------------- 收妥项资格

def _received_item_problem(
    conn: sqlite3.Connection,
    sample: sqlite3.Row,
    batch_item: sqlite3.Row,
    transfer_row: sqlite3.Row,
    occurred_at: str,
    receiver: str,
    actual_seal_no: str,
) -> tuple[str, str] | None:
    """校验单个收妥项是否合格。返回 None=合格；否则返回 (明细 code, message)。

    合格条件（与单份签收规则一致）：
      1. 批次内该笔移交的原事件仍有效（未被作废/替换）；
      2. 该事件是当前保管链上唯一在途（已移交未合格签收）的移交事件——
         由 evaluate 的 pending_transfer.event_id 精确匹配保证
         （移交人非当前保管人的断链移交、重复/在途再移交都不会成为在途事件）；
      3. 签收人 = 该笔移交指定接收人；
      4. 实核封签号 = 样本登记封签号；
      5. 签收时间晚于该笔移交时间。
    """
    index = batch_item["item_index"]
    code = batch_item["sample_code"]

    # 1) 原批次移交事件必须仍有效
    if transfer_row["superseded_by"] is not None:
        return (
            "batch_event_not_effective",
            f"第 {index + 1} 份（样本 {code}）的批次移交事件 {batch_item['event_id']}"
            f" 已被纠错 {transfer_row['superseded_by']} 作废/替换，不再是仍有效的在途移交",
        )

    result = evaluate(conn, sample)
    open_transfer = result["pending_transfer"]

    # 2) 必须处于该笔移交的在途状态
    if open_transfer is None or open_transfer.get("event_id") != batch_item["event_id"]:
        return (
            "transfer_not_in_transit",
            f"第 {index + 1} 份（样本 {code}）的移交单 {batch_item['transfer_no']}"
            " 当前不处于在途状态（已签收/已失效/保管链无在途移交），不能按收妥项签收",
        )

    # 3) 签收人 = 移交指定接收人
    if receiver != open_transfer["to_custodian"]:
        return (
            "receiver_mismatch",
            f"第 {index + 1} 份（样本 {code}）签收人 {receiver} 与移交单"
            f" {batch_item['transfer_no']} 指定接收人 {open_transfer['to_custodian']} 不一致",
        )

    # 4) 封签号与登记一致
    if actual_seal_no != sample["seal_no"]:
        return (
            "seal_mismatch",
            f"第 {index + 1} 份（样本 {code}）实核封签 {actual_seal_no}"
            f" ≠ 登记封签 {sample['seal_no']}",
        )

    # 5) 签收时间晚于移交时间
    if occurred_at <= open_transfer["occurred_at"]:
        return (
            "receive_time_invalid",
            f"第 {index + 1} 份（样本 {code}）签收时间 {occurred_at}"
            f" 不晚于移交时间 {open_transfer['occurred_at']}（单号 {batch_item['transfer_no']}）",
        )
    return None


# --------------------------------------------------------------------------- 提交

def submit_batch_reconciliation(
    conn: sqlite3.Connection, batch_id: str, data: ReconciliationIn
) -> tuple[dict[str, Any], bool]:
    """提交批次对账。返回 (对账查询视图, 是否幂等重试)。"""
    occurred, request_hash = _request_fingerprint(batch_id, data)

    # 1) 批次必须存在（与案卷复核接口一致：资源 404 优先于单号冲突判定）
    batch = _get_batch(conn, batch_id)

    # 2) 同签收单号：同内容幂等 / 异内容冲突（含跨批次复用；失败不占用单号）。
    existing_receipt = _get_reconciliation_by_receipt(conn, data.receipt_no)
    if existing_receipt is not None:
        if existing_receipt["request_hash"] != request_hash:
            raise DomainError(
                409,
                "reconciliation_conflict",
                f"签收单号 {data.receipt_no} 已使用但提交内容不一致（同单号异内容冲突）",
                {"receipt_no": data.receipt_no,
                 "existing_batch_id": existing_receipt["batch_id"],
                 "existing_request_hash": existing_receipt["request_hash"],
                 "incoming_batch_id": batch_id,
                 "incoming_request_hash": request_hash},
            )
        view = get_batch_reconciliation(conn, existing_receipt["batch_id"])
        return view, True

    # 3) 一个批次只能成功对账一次
    existing_batch = _get_reconciliation_by_batch(conn, batch["id"])
    if existing_batch is not None:
        raise DomainError(
            409,
            "batch_reconciled",
            f"批次 {batch_id} 已成功对账（签收单号 {existing_batch['receipt_no']}），"
            "一个批次只能对账一次",
            {"batch_id": batch_id, "existing_receipt_no": existing_batch["receipt_no"]},
        )

    batch_item_rows = _batch_items(conn, batch["id"])
    batch_items_by_sample = {r["sample_code"]: r for r in batch_item_rows}

    # 4) 清单结构校验：重复份项 / 外来项 / 缺项（422）。
    invalid_items: list[dict[str, Any]] = []
    conflict_items: list[dict[str, Any]] = []
    seen_samples: dict[str, int] = {}
    seen_events: dict[str, int] = {}
    incoming_by_sample: dict[str, Any] = {}

    for idx, it in enumerate(data.items):
        if it.sample_code in seen_samples:
            invalid_items.append({
                "index": idx, "sample_code": it.sample_code,
                "event_id": it.event_id, "code": "duplicate_sample_in_reconciliation",
                "message": f"对账清单样本编号重复：{it.sample_code}"
                           f"（第 {seen_samples[it.sample_code] + 1} 份与第 {idx + 1} 份）",
            })
        else:
            seen_samples[it.sample_code] = idx
            incoming_by_sample[it.sample_code] = it

        if it.kind == KIND_RECEIVED:
            if it.event_id in seen_events:
                invalid_items.append({
                    "index": idx, "sample_code": it.sample_code,
                    "event_id": it.event_id, "code": "duplicate_event_in_reconciliation",
                    "message": f"对账清单收妥项事件号重复：{it.event_id}"
                               f"（第 {seen_events[it.event_id] + 1} 份与第 {idx + 1} 份）",
                })
            else:
                seen_events[it.event_id] = idx

        if it.sample_code not in batch_items_by_sample:
            invalid_items.append({
                "index": idx, "sample_code": it.sample_code,
                "event_id": it.event_id, "code": "unknown_sample_in_reconciliation",
                "message": f"外来项：样本 {it.sample_code} 不属于批次 {batch_id}",
            })

    for bit in batch_item_rows:
        if bit["sample_code"] not in incoming_by_sample:
            invalid_items.append({
                "index": None, "sample_code": bit["sample_code"],
                "event_id": None, "code": "missing_sample_in_reconciliation",
                "message": f"缺项：批次 {batch_id} 中的样本 {bit['sample_code']} 未出现在对账清单",
            })

    # 5) 收妥项事件号占用（409 冲突，优先于 422 不合格项聚合）
    if not invalid_items:
        for idx, it in enumerate(data.items):
            if it.kind != KIND_RECEIVED:
                continue
            occupied = conn.execute(
                "SELECT 1 FROM custody_event WHERE event_id = ?", (it.event_id,)
            ).fetchone()
            if occupied is not None:
                conflict_items.append({
                    "index": idx, "sample_code": it.sample_code,
                    "event_id": it.event_id, "code": "event_id_occupied",
                    "message": f"收妥项事件号 {it.event_id} 已被占用",
                })

    # 6) 收妥项资格校验（422；签收人/封签/时间/在途/仍有效）
    if not invalid_items and not conflict_items:
        for idx, it in enumerate(data.items):
            if it.kind != KIND_RECEIVED:
                continue
            bit = batch_items_by_sample[it.sample_code]
            sample = conn.execute(
                "SELECT * FROM sample WHERE id = ?", (bit["sample_id"],)
            ).fetchone()
            transfer_row = conn.execute(
                "SELECT * FROM custody_event WHERE id = ?", (bit["event_row_id"],)
            ).fetchone()
            problem = _received_item_problem(
                conn, sample, bit, transfer_row, occurred, data.receiver, it.actual_seal_no
            )
            if problem is not None:
                code, message = problem
                invalid_items.append({
                    "index": idx, "sample_code": it.sample_code,
                    "event_id": it.event_id, "code": code, "message": message,
                })

    if conflict_items or invalid_items:
        if conflict_items:
            raise DomainError(
                409,
                "reconciliation_rejected",
                f"批次 {batch_id} 对账存在冲突项（收妥事件号占用），整单拒绝且不写入任何对账或签收事件",
                {"batch_id": batch_id, "receipt_no": data.receipt_no,
                 "items": conflict_items + invalid_items},
            )
        raise DomainError(
            422,
            "reconciliation_rejected",
            f"批次 {batch_id} 对账存在不合法项（缺项/重复/外来项/收妥项不合格），整单拒绝且不留记录",
            {"batch_id": batch_id, "receipt_no": data.receipt_no, "items": invalid_items},
        )

    # 7) 全部通过：对账头、各收妥项既有 receive 事件、对账明细、各收妥样本新版本
    #    在同一事务提交；任一步骤失败整体回滚。IMMEDIATE 写锁 + 唯一键
    #    （receipt_no 全局唯一、batch_pk 唯一、event_id 全局唯一）保证并发原子性。
    created_at = utcnow_iso()
    received = [it for it in data.items if it.kind == KIND_RECEIVED]
    exceptions = [it for it in data.items if it.kind == KIND_EXCEPTION]

    try:
        hc = conn.execute(
            "INSERT INTO batch_reconciliation"
            " (batch_pk, batch_id, receipt_no, receiver, occurred_at, item_count,"
            "  received_count, exception_count, request_hash, created_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?)",
            (batch["id"], batch_id, data.receipt_no, data.receiver, occurred,
             len(data.items), len(received), len(exceptions), request_hash, created_at),
        )
    except sqlite3.IntegrityError:
        # 并发竞争：重新读取按已提交状态判定（幂等 / 冲突 / 批次已对账）
        row = _get_reconciliation_by_receipt(conn, data.receipt_no)
        if row is not None:
            if row["request_hash"] == request_hash:
                return get_batch_reconciliation(conn, row["batch_id"]), True
            raise DomainError(
                409, "reconciliation_conflict",
                f"签收单号 {data.receipt_no} 并发提交且内容冲突")
        other = _get_reconciliation_by_batch(conn, batch["id"])
        raise DomainError(
            409, "batch_reconciled",
            f"批次 {batch_id} 并发下已完成对账（签收单号 {other['receipt_no'] if other else '?'}）")

    recon_pk = hc.lastrowid

    for idx, it in enumerate(data.items):
        bit = batch_items_by_sample[it.sample_code]
        if it.kind == KIND_RECEIVED:
            sample = conn.execute(
                "SELECT * FROM sample WHERE id = ?", (bit["sample_id"],)
            ).fetchone()
            event = EventInLike(
                event_id=it.event_id,
                sample_code=it.sample_code,
                occurred_at=data.occurred_at,
                transfer_no=bit["transfer_no"],
                receiver=data.receiver,
                seal_no=it.actual_seal_no,
            )
            canonical, payload = _event_canonical(event)
            h = _fingerprint(canonical)
            payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
            try:
                ec = conn.execute(
                    "INSERT INTO custody_event"
                    " (sample_id, event_id, event_type, occurred_at, payload_json, content_hash,"
                    "  created_at, superseded_by, replaces_event_id, batch_id)"
                    " VALUES (?,?,?,?,?,?,?,NULL,NULL,NULL)",
                    (sample["id"], it.event_id, "receive", occurred, payload_json, h, created_at),
                )
            except sqlite3.IntegrityError:
                raise DomainError(
                    409,
                    "reconciliation_rejected",
                    f"批次 {batch_id} 对账并发冲突：收妥事件号 {it.event_id} 已被占用，整单拒绝",
                    {"batch_id": batch_id, "receipt_no": data.receipt_no,
                     "items": [{"index": idx, "sample_code": it.sample_code,
                                "event_id": it.event_id, "code": "event_id_occupied"}]},
                )
            conn.execute(
                "INSERT INTO batch_reconciliation_item"
                " (reconciliation_pk, batch_item_pk, item_index, sample_id, sample_code,"
                "  transfer_no, kind, event_id, event_row_id, actual_seal_no, exception_reason)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (recon_pk, bit["id"], bit["item_index"], sample["id"], it.sample_code,
                 bit["transfer_no"], KIND_RECEIVED, it.event_id, ec.lastrowid,
                 it.actual_seal_no, None),
            )
            # 收妥项成为既有 receive 事件 → 该样本版本递增（与单事件上报同类）；
            # 异常项不产生事件，不递增版本。各样本版本与事件同一事务，一同提交或回滚。
            record_version(conn, sample, CHANGE_EVENT, it.event_id)
        else:
            conn.execute(
                "INSERT INTO batch_reconciliation_item"
                " (reconciliation_pk, batch_item_pk, item_index, sample_id, sample_code,"
                "  transfer_no, kind, event_id, event_row_id, actual_seal_no, exception_reason)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (recon_pk, bit["id"], bit["item_index"], bit["sample_id"], it.sample_code,
                 bit["transfer_no"], KIND_EXCEPTION, None, None, None, it.reason.strip()),
            )

    return get_batch_reconciliation(conn, batch_id), False


# --------------------------------------------------------------------------- 查询

def _reconciliation_item_view(
    conn: sqlite3.Connection, row: sqlite3.Row
) -> dict[str, Any]:
    """对账明细的对外结构。

    收妥/异常的原始提交内容永久留痕；现时字段（event_effective / superseded_by /
    current_event_id / evaluation）按查询时有效链即时计算——纠错后反映有效性，
    但不改写原对账。
    """
    view: dict[str, Any] = {
        "index": row["item_index"],
        "sample_code": row["sample_code"],
        "transfer_no": row["transfer_no"],
        "kind": row["kind"],
    }
    sample = conn.execute("SELECT * FROM sample WHERE id = ?", (row["sample_id"],)).fetchone()
    if row["kind"] == KIND_RECEIVED:
        receive_row = conn.execute(
            "SELECT * FROM custody_event WHERE id = ?", (row["event_row_id"],)
        ).fetchone()
        result = evaluate(conn, sample)
        view.update({
            "event_id": row["event_id"],
            "actual_seal_no": row["actual_seal_no"],
            "exception_reason": None,
            # 收妥事件的现时有效性（原对账记录不变）
            "event_effective": receive_row["superseded_by"] is None,
            "superseded_by": receive_row["superseded_by"],
            "current_event_id": _effective_event_id(conn, row["event_row_id"]),
            "evaluation": _batch_item_summary(conn, result, sample),
        })
    else:
        result = evaluate(conn, sample)
        view.update({
            "event_id": None,
            "actual_seal_no": None,
            "exception_reason": row["exception_reason"],
            "event_effective": None,
            "superseded_by": None,
            "current_event_id": None,
            "evaluation": _batch_item_summary(conn, result, sample),
        })
    return view


def _reconciliation_view(conn: sqlite3.Connection, header: sqlite3.Row) -> dict[str, Any]:
    item_rows = conn.execute(
        "SELECT * FROM batch_reconciliation_item WHERE reconciliation_pk = ? ORDER BY item_index ASC",
        (header["id"],),
    ).fetchall()
    return {
        "batch_id": header["batch_id"],
        "receipt_no": header["receipt_no"],
        "receiver": header["receiver"],
        "occurred_at": header["occurred_at"],
        "item_count": header["item_count"],
        "received_count": header["received_count"],
        "exception_count": header["exception_count"],
        "created_at": header["created_at"],
        "items": [_reconciliation_item_view(conn, r) for r in item_rows],
    }


def get_batch_reconciliation(conn: sqlite3.Connection, batch_id: str) -> dict[str, Any]:
    """按批次查询成功对账：当时收妥/异常明细 + 各收妥事件现时有效性。

    批次不存在 → 404 batch_not_found；批次存在但未对账 → 404 reconciliation_not_found。
    """
    batch = _get_batch(conn, batch_id)
    header = _get_reconciliation_by_batch(conn, batch["id"])
    if header is None:
        raise DomainError(
            404, "reconciliation_not_found",
            f"批次 {batch_id} 尚未成功对账",
            {"batch_id": batch_id},
        )
    return _reconciliation_view(conn, header)


def reconciliation_summary(conn: sqlite3.Connection, batch_pk: int) -> dict[str, Any] | None:
    """批次查询中的对账摘要（增量字段；未对账为 None）。"""
    header = _get_reconciliation_by_batch(conn, batch_pk)
    if header is None:
        return None
    return {
        "receipt_no": header["receipt_no"],
        "receiver": header["receiver"],
        "occurred_at": header["occurred_at"],
        "received_count": header["received_count"],
        "exception_count": header["exception_count"],
        "reconciled_at": header["created_at"],
    }


# --------------------------------------------------------------------------- 内部适配

class EventInLike:
    """构造收妥项既有 receive 事件时适配 _event_canonical 的轻量入参（不经过 HTTP 校验）。

    _event_canonical 仅读取 event_id / event_type / sample_code / occurred_at /
    transfer_no / receiver / seal_no / note 字段；note 恒为 None（对账事件不带备注）。
    """

    def __init__(self, event_id: str, sample_code: str, occurred_at: Any,
                 transfer_no: str, receiver: str, seal_no: str):
        self.event_id = event_id
        self.event_type = "receive"
        self.sample_code = sample_code
        self.occurred_at = occurred_at
        self.transfer_no = transfer_no
        self.receiver = receiver
        self.seal_no = seal_no
        self.note = None
        # 其余事件类型字段不被 receive 分支读取，占位为 None 以保持属性完整
        self.from_custodian = None
        self.to_custodian = None
        self.checked_seal_no = None
        self.seal_intact = None
        self.custodian = None
        self.storage_location = None
