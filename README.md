# 海上搜救协调系统

标准库实现的独立协调原型，使用 SQLite 保存事件、搜救资源、搜索区域、线索、离线批次、扫测对账和时间线。

## 运行

要求 Python 3.11+（在当前 Python 3.9 环境也可运行）。

```bash
python3 app.py --init --seed
python3 app.py
```

默认地址为 `http://127.0.0.1:8206`，数据库默认为 `maritime_sar.db`。`--db`、`--host`、`--port` 可覆盖默认值。

## 主要接口

写操作使用 JSON，并需要 `X-User` 与 `X-Role` 请求头。角色包括 `coordinator`、`operator`、`field`、`analyst`、`viewer`。

- `GET /health`、`GET /api/state`
- `POST /api/incidents`：创建遇险事件并识别重复报警
- `POST /api/assets`：登记资源
- `POST /api/areas`：创建搜索区域
- `POST /api/assignments`：按能力、海况和航程分配资源
- `POST /api/areas/reassign`：改派搜索区域（释放原资源、占用新资源，旧对账结论失效）
- `POST /api/clues`、`POST /api/clues/verify`
- `POST /api/assets/withdraw`：撤回资源并释放任务
- `POST /api/incidents/transfer`、`POST /api/incidents/close`
- `POST /api/offline/batch`：幂等合并离线记录（线索、时间线、扫测）
- `GET /api/offline/batches`、`GET /api/offline/records?batch=`：批次与逐条台账
- `GET /api/recon?incident_id=&status=`、`POST /api/recon/resolve`：对账复核与重新确认
- `GET /api/incidents/{id}/timeline`

## 回传对账

外勤离线终端的扫测记录随批次回传（`type: "sweep"`，携带记录时的 `area_version` 与 `asset_id`）：

- 同一 `client_event_id` 只入一次；重投返回 `duplicate`，不重复入账。
- 与当前调度一致时直接合并，累积区域 `swept_pct`；区域已改派、资源已释放、区域或事件已结束时，现场数据保留在 `sweep_records`，对账项停在 `pending_review`，列出两端值与差异原因，不覆盖当前调度。
- 区域版本变化（分配、改派、撤回、结束）后，已有复核结论自动失效为 `stale`，需协调员重新确认；确认时按 `applied` 标记保证覆盖率不重复入账。
- 批次内部分失败只重试失败记录：按原 `client_batch_id` 重投修正后的记录即可，批次状态为 `partial` 直到全部接收。
- 库结构按 `PRAGMA user_version` 迁移；旧库升级后旧批次结果回填进台账，对账状态照常可查。

## 结构

数据层 `recon_store.py`（迁移与存取）、对账规则 `recon_rules.py`（纯函数裁决）、接口编排 `app.py`（服务与 HTTP）分开维护，仅使用标准库，不引入外部依赖。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

测试覆盖完整协调流程、重复报警、错误位置、资源并发占用、离线幂等和权限拒绝，以及扫测对账：迟到记录待复核、结论失效重确认、部分失败重试和旧库升级回填。

## 局限

身份依赖调用方传入的用户和角色头；坐标使用球面距离近似；不会自动计算漂移概率区；文件附件、气象服务、真实通信链路和地理围栏未包含在内。
