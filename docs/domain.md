# 领域约定

表达护具认证、批次谱系、平台流通和强制召回之间的领域事件。

聚合对象包括 `producer`、`helmet_model`、`certification_record`、`test_sample`、`product_lot`、`marketing_material`、`shop`、`sale_receipt`、`return_receipt`、`message`、`recall_campaign`、`unit`。所有发生时间都必须携带时区，版本号按聚合从 1 开始递增，基础校验不会改写调用方输入。

## 事件载荷

- `CERTIFICATE_SUBMITTED` / `CERTIFICATE_VERIFIED`：载荷还需包含 `certificate_ref`, `valid_until`。
- `TEST_RESULT_RECORDED`：载荷还需包含 `sample_ref`, `conclusion`。
- `LOT_FROZEN`：载荷还需包含 `affected_units`, `risk_reason`。
- `SALE_RECORDED` / `RETURN_RECORDED`：载荷还需包含 `message_id`, `serials`。
- `MESSAGE_QUARANTINED`：载荷还需包含 `message_id`, `reason`。
- `SCOPE_ADJUSTED`：载荷还需包含 `added`, `removed`。
- `UNIT_DISPOSED`：载荷还需包含 `unit_ref`, `disposition`。
- 其余事件的载荷必填项见 `contracts/domain.schema.json` 的 `payload_required_by_event`。

## 协同服务

`src/helmet_recall/service.py` 在契约之上提供业务语义，所有写操作先落 JSONL 日志再应用，服务重启后重放日志即可继续未完成的召回。

- **角色分权**：认证核验（`certifier`）、检测结论（`tester`）与处置批准（`regulator`）由不同角色完成；商家提交的宣传或申诉材料只登记存档，不能自行解除冻结，解冻只能由监管员执行。
- **高风险处置**：检测不合格或监管定为高风险时，先冻结该批次及拆分、合并出的全部下游批次的未售库存，再沿谱系定位已售产品与责任主体（生产主体、店铺、平台）。
- **检测更正**：更正作废旧结论并只调整受影响范围（`SCOPE_ADJUSTED`）；已发出的通知记录只增不改，冻结状态仍需监管员解冻。
- **幂等与隔离**：销售与退回消息按 `message_id` 幂等，重复投递返回首次结果；编号相同而序列、数量或去向不同的消息进入隔离区，不重复入账，由监管视图呈现。
- **去向仲裁**：退货、销毁、换货并发时，每件产品只保留一个最终去向；相同去向重放幂等，不同去向记为冲突并保留先到者。
- **可控时钟**：`ManualClock` 驱动停售执行、通知逾期与响应逾期升级、证书到期；时间只能显式推进。
- **分角色视图**：监管员可见处置覆盖缺口、隔离消息与升级记录；平台与商家只获取职责范围内的数据；消费者输入序列号、批次号或店铺名（含历史别名）即可得到当前风险、处理方式及其依据。
