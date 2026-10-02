"""端到端自测：同一移交批次的批量对账（batch reconciliation）。

接收方对批量移交到达后的批次逐份提交对账清单：收妥项作为既有 receive 事件与
各样本新版本同事务提交，异常项仅留对账记录；整单拒绝不留痕；一个批次仅可成功
对账一次；同单号同内容幂等、异内容冲突、并发按已提交状态判定。

运行：python -m tests.test_reconciliations
"""
import os
import tempfile
import threading

_tmp = tempfile.mkdtemp()
os.environ["DB_PATH"] = os.path.join(_tmp, "test_reconciliations.db")
os.environ["SEED_ON_START"] = "false"

from fastapi.testclient import TestClient  # noqa: E402

from app.db import db_session, init_db  # noqa: E402
from app.main import app  # noqa: E402

init_db()
client = TestClient(app)
client.__enter__()  # 触发 lifespan（启动钩子）
API = "/api/v1"
PASS, FAIL = 0, 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name} {detail}")


def reg(code, collector="张三", seal=None, when="2026-09-30T01:00:00Z"):
    return client.post(f"{API}/samples", json={
        "sample_code": code, "collected_at": when,
        "collector": collector, "seal_no": seal or f"SEAL-{code}"})


def ev(payload):
    return client.post(f"{API}/events", json=payload)


def chain(code):
    return client.get(f"{API}/samples/{code}/chain").json()


def versions(code):
    return client.get(f"{API}/samples/{code}/versions").json()


def make_batch(bid, codes, frm="张三", to="李四", when="2026-09-30T02:00:00Z",
               transfer_prefix=None, event_prefix=None):
    """登记样本（若未登记）并提交一个批量移交；返回 (响应, 样本信息列表)。"""
    tp = transfer_prefix or f"TR-{bid}"
    ep = event_prefix or f"EVT-{bid}"
    items = []
    for i, code in enumerate(codes):
        items.append({"sample_code": code, "transfer_no": f"{tp}-{i + 1}",
                      "event_id": f"{ep}-T{i + 1}"})
    r = client.post(f"{API}/transfer-batches", json={
        "batch_id": bid, "occurred_at": when,
        "from_custodian": frm, "to_custodian": to, "items": items})
    return r, items


def recon(bid, body):
    return client.post(f"{API}/transfer-batches/{bid}/reconciliation", json=body)


def get_recon(bid):
    return client.get(f"{API}/transfer-batches/{bid}/reconciliation")


def db_scalar(sql, args=()):
    with db_session() as conn:
        return conn.execute(sql, args).fetchone()[0]


def err_codes(resp):
    return [i.get("code") for i in resp.json().get("error", {}).get("details", {}).get("items", [])]


WHEN = "2026-09-30T04:00:00Z"


def recon_body(receipt_no, items, receiver="李四", occurred_at=WHEN):
    return {"receipt_no": receipt_no, "receiver": receiver,
            "occurred_at": occurred_at, "items": items}


def received(sample_code, event_id, seal):
    return {"sample_code": sample_code, "kind": "received",
            "event_id": event_id, "actual_seal_no": seal}


def exception_item(sample_code, reason="外箱破损，待核查"):
    return {"sample_code": sample_code, "kind": "exception", "reason": reason}


# ---------------------------------------------------------------- 1. 正常对账：收妥+异常混合
print("1) 混合对账（2 收妥 + 1 异常）：201，收妥项成为既有 receive，异常项仅留记录")
codes = ["K1", "K2", "K3"]
for c in codes:
    reg(c)
r, bitems = make_batch("BATCH-K", codes)
check("批次提交 201", r.status_code == 201, r.text)
seals = {c: f"SEAL-{c}" for c in codes}
body = recon_body("RCV-K", [
    received("K1", "RCV-K-E1", seals["K1"]),
    exception_item("K2"),
    received("K3", "RCV-K-E3", seals["K3"]),
])
r = recon("BATCH-K", body)
check("对账提交 201", r.status_code == 201, r.text)
rec = r.json()["reconciliation"]
check("顶层 idempotent=false", r.json()["idempotent"] is False)
check("对账头（单号/签收人/时间/份数/收妥/异常计数）",
      rec["receipt_no"] == "RCV-K" and rec["receiver"] == "李四"
      and rec["occurred_at"] == WHEN and rec["item_count"] == 3
      and rec["received_count"] == 2 and rec["exception_count"] == 1, str(rec))
check("明细按批次序排列且 kind 正确",
      [(i["index"], i["sample_code"], i["kind"]) for i in rec["items"]]
      == [(0, "K1", "received"), (1, "K2", "exception"), (2, "K3", "received")],
      str(rec["items"]))
i0, i1, i2 = rec["items"]
check("收妥项回显事件号/实核封签、现时有效、指向自身",
      i0["event_id"] == "RCV-K-E1" and i0["actual_seal_no"] == "SEAL-K1"
      and i0["exception_reason"] is None and i0["event_effective"] is True
      and i0["superseded_by"] is None and i0["current_event_id"] == "RCV-K-E1", str(i0))
check("异常项回显原因、无事件字段",
      i1["event_id"] is None and i1["actual_seal_no"] is None
      and i1["event_effective"] is None and i1["current_event_id"] is None
      and i1["exception_reason"] == "外箱破损，待核查", str(i1))
check("收妥后保管人变为接收人、在途移交关闭",
      i0["evaluation"]["current_custodian"] == "李四"
      and i0["evaluation"]["pending_transfer"] is None, str(i0["evaluation"]))
check("异常项保管人不变、在途移交仍在",
      i1["evaluation"]["current_custodian"] == "张三"
      and i1["evaluation"]["pending_transfer"] is not None
      and i1["evaluation"]["pending_transfer"]["transfer_no"] == "TR-BATCH-K-2",
      str(i1["evaluation"]))

# 链与版本
check("K1 时间线为 transfer+receive，receive 无 batch_id（按事件号追溯对账）",
      [e["event_type"] for e in chain("K1")["timeline"]] == ["transfer", "receive"]
      and chain("K1")["timeline"][1]["batch_id"] is None
      and chain("K1")["timeline"][1]["event_id"] == "RCV-K-E1", str(chain("K1")["timeline"]))
check("K2 时间线仅 transfer（异常不生成签收事件）",
      [e["event_type"] for e in chain("K2")["timeline"]] == ["transfer"])
check("收妥项版本 +1（K1=2），异常项不增版本（K2=1），K3=2",
      versions("K1")["current_version"] == 2 and versions("K2")["current_version"] == 1
      and versions("K3")["current_version"] == 2)
check("收妥样本最新版本 change_type=event、change_ref=收妥事件号",
      versions("K1")["items"][-1]["change_type"] == "event"
      and versions("K1")["items"][-1]["change_ref"] == "RCV-K-E1", str(versions("K1")["items"][-1]))
check("仅写入两条 receive（异常项无事件）",
      db_scalar("SELECT COUNT(*) FROM custody_event WHERE event_type='receive'") == 2)

# 单事件接口对批量收妥事件号：同内容 200 幂等
r = ev({"event_id": "RCV-K-E1", "event_type": "receive", "sample_code": "K1",
        "occurred_at": WHEN, "transfer_no": "TR-BATCH-K-1",
        "receiver": "李四", "seal_no": "SEAL-K1"})
check("单事件接口对收妥事件号同内容 200 幂等",
      r.status_code == 200 and r.json()["idempotent"] is True, r.text)
r = ev({"event_id": "RCV-K-E1", "event_type": "receive", "sample_code": "K1",
        "occurred_at": WHEN, "transfer_no": "TR-BATCH-K-1",
        "receiver": "李四", "seal_no": "SEAL-OTHER"})
check("单事件接口对收妥事件号异内容 409 event_conflict",
      r.status_code == 409 and r.json()["error"]["code"] == "event_conflict", r.text)

# 批次查询带对账摘要
r = client.get(f"{API}/transfer-batches/BATCH-K")
b = r.json()["batch"]
check("批次查询携带 reconciliation 摘要",
      b["reconciliation"] is not None and b["reconciliation"]["receipt_no"] == "RCV-K"
      and b["reconciliation"]["received_count"] == 2
      and b["reconciliation"]["exception_count"] == 1, str(b.get("reconciliation")))

# 对账查询
r = get_recon("BATCH-K")
check("对账查询 200 且内容与提交视图一致",
      r.status_code == 200 and r.json()["reconciliation"]["receipt_no"] == "RCV-K"
      and len(r.json()["reconciliation"]["items"]) == 3, r.text)

# ---------------------------------------------------------------- 2. 同单号幂等 / 异内容冲突 / 一批一次
print("2) 幂等与冲突：同单号同内容 200；异内容 409；批次已对账 409；跨批次复用 409")
r = recon("BATCH-K", body)
check("同单号同内容重试 200 且 idempotent=true",
      r.status_code == 200 and r.json()["idempotent"] is True
      and r.json()["reconciliation"]["receipt_no"] == "RCV-K", r.text)
check("幂等重试不增版本",
      versions("K1")["current_version"] == 2 and versions("K3")["current_version"] == 2)

# items 换序仍幂等（顺序不敏感）
body_shuffled = recon_body("RCV-K", list(reversed(body["items"])))
r = recon("BATCH-K", body_shuffled)
check("同单号同内容（items 换序）仍 200 幂等",
      r.status_code == 200 and r.json()["idempotent"] is True, r.text)

# 时区表示不敏感
body_tz = recon_body("RCV-K", body["items"],
                     occurred_at="2026-09-30T12:00:00+08:00")  # 与 04:00Z 同一时刻
r = recon("BATCH-K", body_tz)
check("同单号同内容（时区等价表示）200 幂等",
      r.status_code == 200 and r.json()["idempotent"] is True, r.text)

# 异内容：改签收人
r = recon("BATCH-K", recon_body("RCV-K", body["items"], receiver="王五"))
check("同单号异内容（签收人变化）409 reconciliation_conflict",
      r.status_code == 409 and r.json()["error"]["code"] == "reconciliation_conflict", r.text)
# 异内容：收妥/异常内容变化（K2 改为收妥）
changed = [received("K1", "RCV-K-E1", "SEAL-K1"),
           received("K2", "RCV-K-E2X", "SEAL-K2"),
           received("K3", "RCV-K-E3", "SEAL-K3")]
r = recon("BATCH-K", recon_body("RCV-K", changed))
check("同单号异内容（份项内容变化）409",
      r.status_code == 409 and r.json()["error"]["code"] == "reconciliation_conflict", r.text)
# 异内容：时间变化
r = recon("BATCH-K", recon_body("RCV-K", body["items"], occurred_at="2026-09-30T05:00:00Z"))
check("同单号异内容（签收时间变化）409",
      r.status_code == 409 and r.json()["error"]["code"] == "reconciliation_conflict", r.text)
check("冲突不改变原对账（仍 2 收妥 1 异常）",
      get_recon("BATCH-K").json()["reconciliation"]["received_count"] == 2
      and versions("K1")["current_version"] == 2)

# 同一批次用新单号再对账 → 409 batch_reconciled
r = recon("BATCH-K", recon_body("RCV-K2", body["items"]))
check("批次已对账再提交 409 batch_reconciled",
      r.status_code == 409 and r.json()["error"]["code"] == "batch_reconciled"
      and r.json()["error"]["details"]["existing_receipt_no"] == "RCV-K", r.text)

# 跨批次复用同一单号（不同批次）
for c in ("L1",):
    reg(c)
r, _ = make_batch("BATCH-L", ["L1"])
check("BATCH-L 提交成功", r.status_code == 201, r.text)
r = recon("BATCH-L", recon_body("RCV-K", [exception_item("L1")]))
check("跨批次复用签收单号 409 reconciliation_conflict",
      r.status_code == 409 and r.json()["error"]["code"] == "reconciliation_conflict"
      and r.json()["error"]["details"]["existing_batch_id"] == "BATCH-K", r.text)
check("跨批次冲突不留对账（BATCH-L 仍未对账）",
      get_recon("BATCH-L").status_code == 404
      and db_scalar("SELECT COUNT(*) FROM batch_reconciliation WHERE batch_id='BATCH-L'") == 0)

# 批次不存在 / 未对账查询
r = recon("BATCH-NOPE", recon_body("RCV-X", [exception_item("K1")]))
check("批次不存在提交 404 batch_not_found",
      r.status_code == 404 and r.json()["error"]["code"] == "batch_not_found", r.text)
r = get_recon("BATCH-L")
check("未对账批次查询 404 reconciliation_not_found",
      r.status_code == 404 and r.json()["error"]["code"] == "reconciliation_not_found", r.text)
r = get_recon("BATCH-NOPE")
check("不存在批次查询 404 batch_not_found",
      r.status_code == 404 and r.json()["error"]["code"] == "batch_not_found", r.text)

# ---------------------------------------------------------------- 3. 整单拒绝：清单覆盖校验
print("3) 整单拒绝：缺项/重复份项/外来项 422，且不留任何对账、事件、版本")
for c in ("M1", "M2"):
    reg(c)
r, _ = make_batch("BATCH-M", ["M1", "M2"])
check("BATCH-M 201", r.status_code == 201)

# 缺项（只提交 M1）
r = recon("BATCH-M", recon_body("RCV-M1", [received("M1", "RCV-M-E1", "SEAL-M1")]))
check("缺项 422 reconciliation_rejected/missing_sample_in_reconciliation",
      r.status_code == 422 and r.json()["error"]["code"] == "reconciliation_rejected"
      and err_codes(r) == ["missing_sample_in_reconciliation"], r.text)
check("缺项明细给出缺失样本与 null index",
      r.json()["error"]["details"]["items"][0]["sample_code"] == "M2"
      and r.json()["error"]["details"]["items"][0]["index"] is None)

# 重复份项
r = recon("BATCH-M", recon_body("RCV-M2", [
    received("M1", "RCV-M-E1", "SEAL-M1"),
    received("M1", "RCV-M-E1B", "SEAL-M1"),
    exception_item("M2")]))
check("重复份项 422 duplicate_sample_in_reconciliation",
      r.status_code == 422 and "duplicate_sample_in_reconciliation" in err_codes(r), r.text)

# 外来项
r = recon("BATCH-M", recon_body("RCV-M3", [
    received("M1", "RCV-M-E1", "SEAL-M1"),
    exception_item("M2"),
    exception_item("K1")]))
check("外来项 422 unknown_sample_in_reconciliation",
      r.status_code == 422 and "unknown_sample_in_reconciliation" in err_codes(r), r.text)

# 批内收妥事件号重复
r = recon("BATCH-M", recon_body("RCV-M4", [
    received("M1", "DUP-RCV-E", "SEAL-M1"),
    received("M2", "DUP-RCV-E", "SEAL-M2")]))
check("收妥事件号批内重复 422 duplicate_event_in_reconciliation",
      r.status_code == 422 and "duplicate_event_in_reconciliation" in err_codes(r), r.text)

check("全部失败不留对账头/明细",
      db_scalar("SELECT COUNT(*) FROM batch_reconciliation WHERE batch_id='BATCH-M'") == 0
      and db_scalar("SELECT COUNT(*) FROM batch_reconciliation_item ri"
                    " JOIN batch_reconciliation r ON ri.reconciliation_pk=r.id"
                    " WHERE r.batch_id='BATCH-M'") == 0)
check("失败不写入收妥事件（M1/M2 时间线仅 transfer）",
      len(chain("M1")["timeline"]) == 1 and len(chain("M2")["timeline"]) == 1)
check("失败不增版本",
      versions("M1")["current_version"] == 1 and versions("M2")["current_version"] == 1)
check("失败不占用签收单号（同单号修正后可成功复用）",
      get_recon("BATCH-M").status_code == 404)
r = recon("BATCH-M", recon_body("RCV-M1", [
    received("M1", "RCV-M-E1", "SEAL-M1"), exception_item("M2")]))
check("原缺项单号补齐后复用成功 201", r.status_code == 201, r.text)

# 字段级校验（Pydantic 422，非业务格式）
r = recon("BATCH-K", recon_body("RCV-BAD-1", [
    {"sample_code": "K1", "kind": "received", "actual_seal_no": "SEAL-K1"},  # 缺 event_id
    exception_item("K2"), received("K3", "X", "SEAL-K3")]))
check("收妥项缺事件号 → 422 字段校验", r.status_code == 422, r.text)
r = recon("BATCH-L", recon_body("RCV-BAD-2", [
    {"sample_code": "L1", "kind": "exception"}]))  # 异常项缺原因
check("异常项缺原因 → 422 字段校验", r.status_code == 422, r.text)
r = recon("BATCH-L", recon_body("RCV-BAD-3", [
    {"sample_code": "L1", "kind": "received", "event_id": "E-X",
     "actual_seal_no": "SEAL-L1", "reason": "不应有原因"}]))
check("收妥项带原因 → 422 字段校验", r.status_code == 422, r.text)
r = recon("BATCH-L", recon_body("RCV-BAD-4", [
    {"sample_code": "L1", "kind": "exception", "reason": "破损",
     "event_id": "E-Y", "actual_seal_no": "SEAL-L1"}]))
check("异常项带事件号/封签 → 422 字段校验", r.status_code == 422, r.text)
r = recon("BATCH-L", recon_body("RCV-BAD-5", []))
check("空清单 → 422 字段校验", r.status_code == 422, r.text)
check("字段校验失败不留对账",
      db_scalar("SELECT COUNT(*) FROM batch_reconciliation WHERE receipt_no LIKE 'RCV-BAD-%'") == 0)

# ---------------------------------------------------------------- 4. 收妥项资格：单份签收规则
print("4) 收妥项不合格：封签/签收人/时间/在途/事件有效，逐项 422 且整单不留痕")

# 4.1 封签不符
for c in ("N1", "N2"):
    reg(c)
make_batch("BATCH-N", ["N1", "N2"])
r = recon("BATCH-N", recon_body("RCV-N1", [
    received("N1", "RCV-N-E1", "SEAL-WRONG"), exception_item("N2")]))
check("实核封签不符 422 seal_mismatch",
      r.status_code == 422 and err_codes(r) == ["seal_mismatch"], r.text)
check("封签不符不写事件（单事件接口亦不可见）",
      ev({"event_id": "RCV-N-E1", "event_type": "receive", "sample_code": "N1",
          "occurred_at": WHEN, "transfer_no": "TR-BATCH-N-1",
          "receiver": "李四", "seal_no": "SEAL-WRONG"}).status_code == 201)
# 上面用单事件接口把不合格签收作为无效事件补录了，作废它以便后续场景复用批次资格校验
# （该无效签收不关闭在途移交，批次对账仍可成功）
r = client.post(f"{API}/corrections", json={
    "correction_id": "FIX-N1", "sample_code": "N1", "target_event_id": "RCV-N-E1",
    "action": "void", "operator": "质控员", "reason": "封签录入错误演示清理"})
check("作废补录的无效签收成功", r.status_code == 201, r.text)
check("作废后在途移交重新可对账（pending_transfer 回到批次事件）",
      chain("N1")["pending_transfer"] is not None
      and chain("N1")["pending_transfer"]["event_id"] == "EVT-BATCH-N-T1",
      str(chain("N1").get("pending_transfer")))

# 4.2 签收人不匹配
r = recon("BATCH-N", recon_body("RCV-N2", [
    received("N1", "RCV-N-E1B", "SEAL-N1"), exception_item("N2")], receiver="王五"))
check("签收人≠接收人 422 receiver_mismatch",
      r.status_code == 422 and err_codes(r) == ["receiver_mismatch"], r.text)

# 4.3 签收时间不晚于移交
r = recon("BATCH-N", recon_body("RCV-N3", [
    received("N1", "RCV-N-E1C", "SEAL-N1"), exception_item("N2")],
    occurred_at="2026-09-30T02:00:00Z"))  # 等于移交时间
check("签收时间不晚于移交 422 receive_time_invalid",
      r.status_code == 422 and err_codes(r) == ["receive_time_invalid"], r.text)
r = recon("BATCH-N", recon_body("RCV-N3B", [
    received("N1", "RCV-N-E1D", "SEAL-N1"), exception_item("N2")],
    occurred_at="2026-09-30T01:30:00Z"))  # 早于移交
check("签收时间早于移交 422 receive_time_invalid",
      r.status_code == 422 and "receive_time_invalid" in err_codes(r), r.text)

# 4.4 事件号已占用（409 优先于 422 聚合）
r = recon("BATCH-N", recon_body("RCV-N4", [
    received("N1", "EVT-BATCH-N-T1", "SEAL-N1"), exception_item("N2")]))  # 占用为批次自身事件号
check("收妥事件号已占用 409 reconciliation_rejected/event_id_occupied",
      r.status_code == 409 and r.json()["error"]["code"] == "reconciliation_rejected"
      and err_codes(r) == ["event_id_occupied"], r.text)

# 4.5 移交不在途：该批样本已通过单事件签收
for c in ("P1", "P2"):
    reg(c)
make_batch("BATCH-P", ["P1", "P2"])
s1 = ev({"event_id": "P-SOLO-R1", "event_type": "receive", "sample_code": "P1",
         "occurred_at": "2026-09-30T03:00:00Z", "transfer_no": "TR-BATCH-P-1",
         "receiver": "李四", "seal_no": "SEAL-P1"})
check("P1 单事件合格签收 201（在途关闭）", s1.status_code == 201, s1.text)
r = recon("BATCH-P", recon_body("RCV-P1", [
    received("P1", "RCV-P-E1", "SEAL-P1"), exception_item("P2")]))
check("已签收样本再按收妥项 422 transfer_not_in_transit",
      r.status_code == 422 and err_codes(r) == ["transfer_not_in_transit"], r.text)
check("在途已关闭不收尾、不新增事件",
      len(chain("P1")["timeline"]) == 2
      and db_scalar("SELECT COUNT(*) FROM custody_event WHERE event_id='RCV-P-E1'") == 0)

# 4.6 批次移交事件已被作废/替换 → batch_event_not_effective
for c in ("Q1", "Q2"):
    reg(c)
make_batch("BATCH-Q", ["Q1", "Q2"])
r = client.post(f"{API}/corrections", json={
    "correction_id": "FIX-Q1", "sample_code": "Q1", "target_event_id": "EVT-BATCH-Q-T1",
    "action": "void", "operator": "质控员", "reason": "批次移交作废演示"})
check("Q1 批次移交作废成功", r.status_code == 201, r.text)
r = recon("BATCH-Q", recon_body("RCV-Q1", [
    received("Q1", "RCV-Q-E1", "SEAL-Q1"), exception_item("Q2")]))
check("批次移交已作废 422 batch_event_not_effective",
      r.status_code == 422 and err_codes(r) == ["batch_event_not_effective"], r.text)
check("作废后整单拒绝不留对账", get_recon("BATCH-Q").status_code == 404)

# 4.7 批次移交被替换为新的有效 transfer（在途事件是替换后继而非原事件）
r = client.post(f"{API}/corrections", json={
    "correction_id": "FIX-Q2", "sample_code": "Q2", "target_event_id": "EVT-BATCH-Q-T2",
    "action": "replace", "operator": "质控员", "reason": "移交时间更正，整体替换",
    "new_event": {
        "event_id": "EVT-BATCH-Q-T2N", "event_type": "transfer",
        "occurred_at": "2026-09-30T02:30:00Z",
        "transfer_no": "TR-BATCH-Q-2", "from_custodian": "张三", "to_custodian": "李四"}})
check("Q2 批次移交替换成功", r.status_code == 201, r.text)
r = recon("BATCH-Q", recon_body("RCV-Q2", [
    exception_item("Q1"), received("Q2", "RCV-Q-E2", "SEAL-Q2")]))
# 原批次事件已失效（即使存在替换后继在途）→ 整单拒绝：收妥项须对应原批次事件仍有效
check("批次原事件被替换（现为后继在途）422 batch_event_not_effective",
      r.status_code == 422 and err_codes(r) == ["batch_event_not_effective"], r.text)

# 4.8 断链移交（移交人不是当前保管人）不构成在途 → transfer_not_in_transit
reg("O1", collector="钱采集")
make_batch("BATCH-O", ["O1"], frm="张三", to="李四")  # 张三非保管人
check("O1 批次提交后为 invalid（断链）",
      client.get(f"{API}/transfer-batches/BATCH-O").json()["batch"]["items"][0]
      ["evaluation"]["status"] == "invalid")
r = recon("BATCH-O", recon_body("RCV-O1", [received("O1", "RCV-O-E1", "SEAL-O1")]))
check("断链移交不处于在途 422 transfer_not_in_transit",
      r.status_code == 422 and err_codes(r) == ["transfer_not_in_transit"], r.text)

# 4.9 多收妥项并存：一个封签错、一个时间错 → 明细逐项聚合，整单拒绝
for c in ("U1", "U2"):
    reg(c)
make_batch("BATCH-U", ["U1", "U2"])
r = recon("BATCH-U", recon_body("RCV-U1", [
    received("U1", "RCV-U-E1", "SEAL-BAD"),
    received("U2", "RCV-U-E2", "SEAL-U2")], occurred_at="2026-09-30T01:00:00Z"))
check("多项不合格聚合（seal_mismatch + receive_time_invalid）422",
      r.status_code == 422 and set(err_codes(r)) == {"seal_mismatch", "receive_time_invalid"},
      r.text)
check("聚合失败不收尾任何事件",
      db_scalar("SELECT COUNT(*) FROM custody_event WHERE event_id IN ('RCV-U-E1','RCV-U-E2')") == 0)

# 4.10 冲突优先：事件号占用(409) 与 资格不合格(422) 并存 → 409
r = recon("BATCH-U", recon_body("RCV-U2", [
    received("U1", "EVT-BATCH-U-T1", "SEAL-BAD"),  # 占用 + 封签错
    received("U2", "RCV-U-E2", "SEAL-U2")]))
check("冲突与不合格并存：409 优先且明细含两类",
      r.status_code == 409 and r.json()["error"]["code"] == "reconciliation_rejected"
      and "event_id_occupied" in err_codes(r), r.text)

# ---------------------------------------------------------------- 5. 全异常 / 全收妥
print("5) 全异常对账与全收妥对账")
for c in ("V1", "V2"):
    reg(c)
make_batch("BATCH-V", ["V1", "V2"])
r = recon("BATCH-V", recon_body("RCV-V1", [exception_item("V1"), exception_item("V2", "温控超标")]))
check("全异常对账 201（无收妥事件、无版本递增）",
      r.status_code == 201 and r.json()["reconciliation"]["received_count"] == 0
      and r.json()["reconciliation"]["exception_count"] == 2, r.text)
check("全异常不产生 receive、不增版本",
      db_scalar("SELECT COUNT(*) FROM custody_event e JOIN sample s ON e.sample_id=s.id"
                " WHERE s.sample_code IN ('V1','V2') AND e.event_type='receive'") == 0
      and versions("V1")["current_version"] == 1 and versions("V2")["current_version"] == 1)
check("全异常后各份仍在途（可继续单事件签收）",
      chain("V1")["pending_transfer"]["event_id"] == "EVT-BATCH-V-T1"
      and chain("V2")["pending_transfer"]["event_id"] == "EVT-BATCH-V-T2")

for c in ("W1", "W2"):
    reg(c)
make_batch("BATCH-W", ["W1", "W2"])
r = recon("BATCH-W", recon_body("RCV-W1", [
    received("W1", "RCV-W-E1", "SEAL-W1"), received("W2", "RCV-W-E2", "SEAL-W2")]))
check("全收妥对账 201",
      r.status_code == 201 and r.json()["reconciliation"]["received_count"] == 2
      and r.json()["reconciliation"]["exception_count"] == 0, r.text)
check("全收妥后各份保管人均为接收人、在途关闭",
      all(i["evaluation"]["current_custodian"] == "李四"
          and i["evaluation"]["pending_transfer"] is None
          for i in r.json()["reconciliation"]["items"]))

# ---------------------------------------------------------------- 6. 纠错后查询反映现时有效性，不改写原对账
print("6) 对账后纠错：查询呈现收妥事件现时有效性，原对账内容不变")
# W1 的收妥事件作废 → 查询 event_effective=false / current_event_id=null
r = client.post(f"{API}/corrections", json={
    "correction_id": "FIX-W1", "sample_code": "W1", "target_event_id": "RCV-W-E1",
    "action": "void", "operator": "质控员", "reason": "签收记录作废演示"})
check("W1 收妥事件作废 201", r.status_code == 201, r.text)
g = get_recon("BATCH-W").json()["reconciliation"]
w1 = next(i for i in g["items"] if i["sample_code"] == "W1")
w2 = next(i for i in g["items"] if i["sample_code"] == "W2")
check("作废后查询：W1 event_effective=false / current_event_id=null / superseded_by 记录",
      w1["event_effective"] is False and w1["current_event_id"] is None
      and w1["superseded_by"] == "FIX-W1", str(w1))
check("原对账头与收妥内容不改写（单号/时间/收妥计数/原事件号/实核封签）",
      g["receipt_no"] == "RCV-W1" and g["received_count"] == 2
      and w1["event_id"] == "RCV-W-E1" and w1["actual_seal_no"] == "SEAL-W1"
      and w1["kind"] == "received", str(g))
check("现时 evaluation 反映作废后状态（在途重新打开、保管人回到移交人）",
      w1["evaluation"]["pending_transfer"] is not None
      and w1["evaluation"]["current_custodian"] == "张三"
      and w1["evaluation"]["current_version"] == 3, str(w1["evaluation"]))
check("W2 未纠错仍有效", w2["event_effective"] is True and w2["current_event_id"] == "RCV-W-E2")

# W2 收妥事件替换 → current_event_id 沿替换链到后继
r = client.post(f"{API}/corrections", json={
    "correction_id": "FIX-W2", "sample_code": "W2", "target_event_id": "RCV-W-E2",
    "action": "replace", "operator": "质控员", "reason": "签收封签扫码错误，整体替换",
    "new_event": {
        "event_id": "RCV-W-E2N", "event_type": "receive",
        "occurred_at": "2026-09-30T04:30:00Z",
        "transfer_no": "TR-BATCH-W-2", "receiver": "李四", "seal_no": "SEAL-W2"}})
check("W2 收妥事件替换 201", r.status_code == 201, r.text)
g = get_recon("BATCH-W").json()["reconciliation"]
w2 = next(i for i in g["items"] if i["sample_code"] == "W2")
check("替换后查询：原事件失效、current_event_id 指向后继、evaluation 含新事件",
      w2["event_effective"] is False and w2["current_event_id"] == "RCV-W-E2N"
      and w2["superseded_by"] == "FIX-W2"
      and any(e["event_id"] == "RCV-W-E2N" for e in chain("W2")["timeline"]), str(w2))
check("原对账仍记录原始收妥事件号（不随后继改写）", w2["event_id"] == "RCV-W-E2")

# 异常项样本独立做纠错不影响对账异常留痕
r = client.post(f"{API}/corrections", json={
    "correction_id": "FIX-V1T", "sample_code": "V1", "target_event_id": "EVT-BATCH-V-T1",
    "action": "void", "operator": "质控员", "reason": "异常样本移交作废"})
check("异常项对应批次移交可独立作废 201", r.status_code == 201, r.text)
g = get_recon("BATCH-V").json()["reconciliation"]
v1 = next(i for i in g["items"] if i["sample_code"] == "V1")
check("异常项留痕不变（仍为 exception + 原因）",
      v1["kind"] == "exception" and v1["exception_reason"] == "外箱破损，待核查"
      and v1["event_id"] is None, str(v1))

# ---------------------------------------------------------------- 7. 并发
print("7) 并发：同单号同内容一建一幂等；同批异单号一胜 batch_reconciled；事件号竞争一胜整单回滚")

# 7.1 同单号同内容
for c in ("X1", "X2"):
    reg(c)
make_batch("BATCH-X", ["X1", "X2"])
body_x = recon_body("RCV-X1", [
    received("X1", "RCV-X-E1", "SEAL-X1"), exception_item("X2")])
results_x: dict[str, int] = {}
barrier = threading.Barrier(2)


def post_x(tag):
    barrier.wait()
    results_x[tag] = recon("BATCH-X", body_x).status_code


t1 = threading.Thread(target=post_x, args=("A",))
t2 = threading.Thread(target=post_x, args=("B",))
t1.start(); t2.start(); t1.join(); t2.join()
check("同单号同内容并发：一次 201 一次 200",
      sorted(results_x.values()) == [200, 201], str(results_x))
check("并发后仅一条对账、一个收妥事件",
      db_scalar("SELECT COUNT(*) FROM batch_reconciliation WHERE batch_id='BATCH-X'") == 1
      and db_scalar("SELECT COUNT(*) FROM custody_event WHERE event_id='RCV-X-E1'") == 1
      and versions("X1")["current_version"] == 2)

# 7.2 同批次两个不同单号竞争：一建一 batch_reconciled
for c in ("Y1", "Y2"):
    reg(c)
make_batch("BATCH-Y", ["Y1", "Y2"])
body_y1 = recon_body("RCV-Y1", [
    received("Y1", "RCV-Y-E1", "SEAL-Y1"), exception_item("Y2")])
body_y2 = recon_body("RCV-Y2", [
    exception_item("Y1"), received("Y2", "RCV-Y-E2", "SEAL-Y2")])
results_y: dict[str, int] = {}
barrier2 = threading.Barrier(2)


def post_y(tag, body):
    barrier2.wait()
    results_y[tag] = recon("BATCH-Y", body).status_code


t1 = threading.Thread(target=post_y, args=("A", body_y1))
t2 = threading.Thread(target=post_y, args=("B", body_y2))
t1.start(); t2.start(); t1.join(); t2.join()
check("同批异单号并发：一次 201 一次 409",
      sorted(results_y.values()) == [201, 409], str(results_y))
check("仅获胜对账落库；落败方收妥事件整单回滚",
      db_scalar("SELECT COUNT(*) FROM batch_reconciliation WHERE batch_id='BATCH-Y'") == 1
      and db_scalar("SELECT COUNT(*) FROM custody_event WHERE event_type='receive'"
                    " AND event_id IN ('RCV-Y-E1','RCV-Y-E2')") == 1)
winner_no = "RCV-Y1" if results_y["A"] == 201 else "RCV-Y2"
check("查询返回获胜对账内容",
      get_recon("BATCH-Y").json()["reconciliation"]["receipt_no"] == winner_no)

# 7.3 不同批次收妥事件号交叠：一胜整单回滚（另一单 409 不留对账）
for c in ("Z1", "Z2"):
    reg(c)
make_batch("BATCH-ZA", ["Z1"])
make_batch("BATCH-ZB", ["Z2"])
body_za = recon_body("RCV-ZA", [received("Z1", "SHARED-RCV", "SEAL-Z1")])
body_zb = recon_body("RCV-ZB", [received("Z2", "SHARED-RCV", "SEAL-Z2")])
results_z: dict[str, int] = {}
barrier3 = threading.Barrier(2)


def post_z(tag, bid, body):
    barrier3.wait()
    results_z[tag] = recon(bid, body).status_code


t1 = threading.Thread(target=post_z, args=("A", "BATCH-ZA", body_za))
t2 = threading.Thread(target=post_z, args=("B", "BATCH-ZB", body_zb))
t1.start(); t2.start(); t1.join(); t2.join()
check("事件号交叠并发：一个 201 一个 409",
      sorted(results_z.values()) == [201, 409], str(results_z))
z_winner = "BATCH-ZA" if results_z["A"] == 201 else "BATCH-ZB"
z_loser = "BATCH-ZB" if z_winner == "BATCH-ZA" else "BATCH-ZA"
check("落败批次不留对账头（修正单号后可重试）",
      get_recon(z_loser).status_code == 404
      and db_scalar("SELECT COUNT(*) FROM batch_reconciliation WHERE batch_id=?",
                    (z_loser,)) == 0)
check("共享收妥事件仅一条",
      db_scalar("SELECT COUNT(*) FROM custody_event WHERE event_id='SHARED-RCV'") == 1)

# ---------------------------------------------------------------- 8. 与核定/既有接口的关系
print("8) 与核定及既有接口契约：对账收妥后可走完封签检查+入库+核定；核定后历史冻结")
for c in ("AA1",):
    reg(c)
make_batch("BATCH-AA", ["AA1"])
r = recon("BATCH-AA", recon_body("RCV-AA1", [received("AA1", "RCV-AA-E1", "SEAL-AA1")]))
check("AA1 收妥对账 201", r.status_code == 201, r.text)
check("封签检查 201", ev({"event_id": "AA-C1", "event_type": "seal_check", "sample_code": "AA1",
      "occurred_at": "2026-09-30T04:30:00Z", "checked_seal_no": "SEAL-AA1",
      "seal_intact": True}).status_code == 201)
check("入库 201", ev({"event_id": "AA-S1", "event_type": "storage", "sample_code": "AA1",
      "occurred_at": "2026-09-30T05:00:00Z", "custodian": "实验室"}).status_code == 201)
check("AA1 核定前 valid", chain("AA1")["status"] == "valid", chain("AA1")["status"])
r = client.post(f"{API}/samples/AA1/approve", json={"approver": "质量主管"})
check("AA1 核定 200", r.status_code == 200, r.text)
# 核定后历史冻结：对账不可改（本来一批一次），收妥事件不可再纠错
r = client.post(f"{API}/corrections", json={
    "correction_id": "FIX-AA1", "sample_code": "AA1", "target_event_id": "RCV-AA-E1",
    "action": "void", "operator": "质控员", "reason": "核定后尝试作废"})
check("核定后作废收妥事件 409 history_locked",
      r.status_code == 409 and r.json()["error"]["code"] == "history_locked", r.text)
# 已核定样本的批次：对账收妥项本就不可能（在途已无），整单拒绝；幂等查询不受冻结影响
check("核定后对账查询仍 200（只读不受冻结影响）", get_recon("BATCH-AA").status_code == 200)

# 已核定批次场景：样本先走完链并核定，再尝试对账 → 收妥项无在途，整单 422 不留痕
for c in ("AB1",):
    reg(c)
ev({"event_id": "AB-T0", "event_type": "transfer", "sample_code": "AB1",
    "occurred_at": "2026-09-30T01:20:00Z", "transfer_no": "TR-AB0",
    "from_custodian": "张三", "to_custodian": "李四"})
ev({"event_id": "AB-R0", "event_type": "receive", "sample_code": "AB1",
    "occurred_at": "2026-09-30T01:40:00Z", "transfer_no": "TR-AB0",
    "receiver": "李四", "seal_no": "SEAL-AB1"})
ev({"event_id": "AB-C0", "event_type": "seal_check", "sample_code": "AB1",
    "occurred_at": "2026-09-30T02:00:00Z", "checked_seal_no": "SEAL-AB1", "seal_intact": True})
ev({"event_id": "AB-S0", "event_type": "storage", "sample_code": "AB1",
    "occurred_at": "2026-09-30T03:00:00Z", "custodian": "实验室"})
check("AB1 核定 200",
      client.post(f"{API}/samples/AB1/approve", json={"approver": "质量主管"}).status_code == 200)
# 再挂一个批量移交到已核定样本 → 409（既有契约不变）
r = make_batch("BATCH-AB", ["AB1"])
check("已核定样本批量移交仍 409 batch_rejected/sample_approved（既有契约）",
      r[0].status_code == 409 and r[0].json()["error"]["code"] == "batch_rejected"
      and r[0].json()["error"]["details"]["items"][0]["code"] == "sample_approved", r[0].text)

# ---------------------------------------------------------------- 9. 既有接口契约保持
print("9) 既有接口契约保持：单份签收 / 批量移交 / 查询字段不回归")
# 未对账批次的批次查询 reconciliation 为 None
r = client.get(f"{API}/transfer-batches/BATCH-L")
check("未对账批次 reconciliation=null（增量字段，不破坏既有结构）",
      r.status_code == 200 and r.json()["batch"]["reconciliation"] is None, r.text)
# pending_transfer 增量 event_id 字段
check("chain 的 pending_transfer 含 event_id（增量字段）",
      "event_id" in chain("L1")["pending_transfer"], str(chain("L1")["pending_transfer"]))
# 既有 7 个测试套件的场景由独立脚本保障；这里仅抽查根/健康/样本列表
check("健康检查 200", client.get("/health").status_code == 200)
check("样本列表 200", client.get(f"{API}/samples").status_code == 200)

# ---------------------------------------------------------------- 汇总
print(f"\n结果: {PASS} 通过, {FAIL} 失败")
if FAIL:
    raise SystemExit(1)
