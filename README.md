# 头部护具召回协同链

表达护具认证、批次谱系、平台流通和强制召回之间的领域事件，并在事件流之上提供召回协同服务。

## 目录

- `contracts/domain.schema.json`：对象、事件和载荷字段约定。
- `data/sample.json`：可直接校验的联调样例。
- `src/helmet_recall/`：契约校验、事件存储、可控时钟与协同服务。
- `tests/`：信封契约与召回业务场景测试。
- `docs/domain.md`：领域对象与事件语义。

## 协同服务能力

`helmet_recall.service.RecallService` 基于纯追加事件流折叠读模型：

- **角色分离**：认证核验（`cert_verifier`）、检测结论（`tester`）、处置批准（`approver`）互不兼任；平台、商家、仓库、消费者各有授权边界。商家提交的材料（`MERCHANT_MATERIAL_SUBMITTED`）只登记在案，**不能自行解除冻结**；解冻只能由批准人在更正后执行。
- **先控库存再追溯**：高风险立案时，先沿批次拆合谱系冻结尚未售出的库存（`LOT_FROZEN`）并同步停售，再按销售回执定位已售产品，沿谱系根批次定位责任生产主体。
- **检测更正不改写历史**：`TEST_RESULT_CORRECTED` 只追加事实、标记原结论并调整召回范围；已执行的通知事件保留可查。
- **幂等与隔离**：相同事件编号重复投递且业务内容一致时幂等忽略；编号相同但序列、数量或去向不同时进入隔离账本（`EventStore.quarantine`），不覆盖原事实。
- **单件唯一最终去向**：并发退货、销毁、换货请求在锁内裁决，每件产品只接受一个待执行去向，竞争请求落 `DISPOSITION_REJECTED`，确认后不得更改。
- **可控时钟**：`ControlledClock` 驱动停售、通知响应期限与逾期升级（`RECALL_ESCALATED`）；`poll_timeouts()` 可重复调用，升级不重复触发。
- **重启恢复**：事件流可写入 JSONL（`EventStore(schema, path)`），服务重启后重放全部事实与隔离记录，继续未完成召回。
- **角色视图**：监管视角给出处置覆盖缺口（已售未通知、待响应、逾期、已响应未办结等）；平台只见下架/通知/库存；商家只见本店回执与措施；消费者凭单件序列查询当前风险、处理方式与依据。

## 测试

```bash
python3 -m unittest discover -s tests
```

## 编译检查

```bash
python3 -m compileall -q src tests
```

## 样例校验

```bash
PYTHONPATH=src python3 -m helmet_recall.cli contracts/domain.schema.json data/sample.json
```

样例有效时输出 `valid`；发现问题时逐行给出字段、代码和中文说明，并返回非零状态。
