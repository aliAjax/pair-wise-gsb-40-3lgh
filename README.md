# 海上搜救协调系统

标准库实现的独立协调原型，使用 SQLite 保存事件、搜救资源、搜索区域、线索、离线批次和时间线。

## 模块划分

- `app.py`：协调主流程与 HTTP 组合根（事件、资源、区域、线索、离线批次）。
- `recon_store.py`：回传对账数据层，负责表结构、版本迁移（`schema_meta`）与 SQL，不依赖其它模块。
- `recon_rules.py`：对账规则层，纯函数决策（合并 / 停待复核及差异原因），不接触数据库。
- `recon_api.py`：对账接口层，负责校验、幂等合并、复核与状态查询，供 HTTP 层调用。

## 运行

要求 Python 3.11+（在当前 Python 3.9 环境也可运行）。

```bash
python3 app.py --init --seed
python3 app.py
```

默认地址为 `http://127.0.0.1:8206`，数据库默认为 `maritime_sar.db`。`--db`、`--host`、`--port` 可覆盖默认值。老库启动时自动迁移到对账结构，升级前的线索和离线批次在对账状态里标记为 `legacy_merged`。

## 主要接口

写操作使用 JSON，并需要 `X-User` 与 `X-Role` 请求头。角色包括 `coordinator`、`operator`、`field`、`analyst`、`viewer`。

- `GET /health`、`GET /api/state`
- `POST /api/incidents`：创建遇险事件并识别重复报警
- `POST /api/assets`：登记资源
- `POST /api/areas`：创建搜索区域
- `POST /api/assignments`：按能力、海况和航程分配资源
- `POST /api/clues`、`POST /api/clues/verify`
- `POST /api/assets/withdraw`：撤回资源并释放任务
- `POST /api/incidents/transfer`、`POST /api/incidents/close`
- `POST /api/offline/batch`：幂等合并离线记录
- `GET /api/incidents/{id}/timeline`

## 回传对账接口

外勤离线终端回网后按批次回传扫测记录，记录携带当时的区域版本 `area_version_seen`：

- `POST /api/recon/batch`：`{"client_batch_id": "...", "records": [{"client_event_id", "incident_id", "area_id", "area_version_seen", "asset_name", "swept_pct", "contacts", "note", "recorded_at"}]}`。同一记录只入一次；区域已改派、资源已释放、区域或事件已结束时保留现场数据并停在 `pending_review`，返回两端值与差异原因；部分失败只重试失败记录（按批次回执去重，已接收不重复）。
- `POST /api/recon/review`：`{"record_id", "conclusion": "confirmed|dismissed", "note"}`，仅协调员。区域版本一旦变化，已有复核结论自动失效（`stale`），记录回到待复核需重新确认。
- `GET /api/recon/status`：对账总览，含待复核列表、批次汇总、失效结论数与升级前历史数据。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

测试覆盖完整协调流程、重复报警、错误位置、资源并发占用、离线幂等、权限拒绝，以及回传对账的幂等入库、区域版本差异、资源释放、事件结束、结论失效重确认、部分失败重试、老库升级和 HTTP 接口。

## 局限

身份依赖调用方传入的用户和角色头；坐标使用球面距离近似；不会自动计算漂移概率区；文件附件、气象服务、真实通信链路和地理围栏未包含在内。
