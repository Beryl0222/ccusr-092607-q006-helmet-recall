# 领域约定

表达护具认证、批次谱系、平台流通和强制召回之间的领域事件。

聚合对象包括 `helmet_model`、`certification_record`、`product_lot`、`recall_campaign`。所有发生时间都必须携带时区，版本号从 1 开始递增，基础校验不会改写调用方输入。

## 事件类型

### 型号与流通

- `MODEL_REGISTERED`：登记生产主体与型号结构（载荷：`producer_ref`、`model_name`、`designated_uses`，标称自行车/电动车/摩托车通用时必须全部列出）。
- `MODEL_LISTING_PUBLISHED`：店铺/主播发布宣传素材与商品链接（载荷：`model_ref`、`shop_ref`、`listing_ref`、`promo_material_refs`、`claimed_uses`）。
- `LISTING_DELISTED`：平台下架商品。下架只切断新销售，不解除批次冻结，也不替代召回。
- `STOCK_MOVED`：库存移动（仓库、主播、线下经销商之间），载荷见下。
- `UNIT_SOLD`：销售回执，记录批次、单件序列、渠道与店铺。

### 认证与检测

- `CERTIFICATE_VERIFIED`：认证核验通过（角色：认证核验员），载荷含 `certificate_ref`、`valid_until`、`model_ref`。
- `CERTIFICATE_REVOKED`：证书撤销/失效。
- `TEST_RESULT_RECORDED`：检测结论（角色：检测机构），载荷含 `sample_ref`、`conclusion`（`pass`/`fail`）。
- `TEST_RESULT_CORRECTED`：检测更正。**只调整受影响范围**，载荷必须引用原检测事件 `test_event_id`；更正不会删除或改写原事件，曾据原结论发出的通知保留可追溯。

### 批次谱系

- `LOT_CREATED`：批次建立，载荷含 `lot_ref`、`producer_ref`、`model_ref`、`quantity`。
- `LOT_SPLIT`：批次拆分为多个批次（换店铺、分批销售），载荷含 `parent_lot_ref`、`child_lot_refs`、`quantities`、`unit_assignments`（各子批次分得的单件序列）。
- `LOT_MERGED`：批次合并，载荷含 `parent_lot_refs`、`child_lot_ref`、`quantity`。
- `LOT_FROZEN`：冻结尚未售出的库存，载荷含 `affected_units`、`risk_reason`。冻结只能由处置批准角色触发；商家提交的材料事件（`MERCHANT_MATERIAL_SUBMITTED`）仅记录在案，不改变冻结状态，不构成解冻依据。
- `LOT_RELEASED`：解除冻结只能由处置批准角色在更正或新证据后执行（载荷：`reason`）。

### 召回处置

- `RECALL_OPENED`：批准立案召回，载荷含 `lot_refs`、`risk_level`、`basis`、`approved_by`、`unsold_control`（先控库存的执行结果）。
- `RECALL_SCOPE_ADJUSTED`：检测更正后调整受影响批次范围，载荷含 `lot_refs`、`reason`。不抹除已发通知。
- `RECALL_NOTICE_SENT`：向已售产品的回执联系人发出通知，载荷含 `notice_ref`、`channel`、`sales_receipt_refs`、`response_deadline`。
- `CONSUMER_RESPONDED`：消费者响应（退货/销毁/换货/无响应），载荷含 `notice_ref`、`unit_ref`、`choice`。
- `RECALL_ESCALATED`：逾期等情形下风险等级升级，载荷含 `from_level`、`to_level`、`reason`。
- `RECALL_CLOSED`：召回闭环。
- `UNIT_DISPOSAL_REQUESTED` / `UNIT_DISPOSED` / `DISPOSITION_REJECTED`：单件产品的处置请求与最终去向（`return`/`destroy`/`exchange`）。每件产品在全部并发退货、销毁、换货中只能有一个最终去向；冲突请求被隔离而非择一覆盖。

## 幂等与隔离

- 同一 `event_id` 重复到达为重复消息：内容一致时幂等忽略。
- `event_id` 相同但业务键对应的序列、数量或去向不同（如同号销售/退回消息），不覆盖原事实，进入冲突隔离账本，等待人工裁决。

## 责任划分

认证核验、检测结论、处置批准分别由不同角色完成；高风险批次先冻结未售库存，再沿批次拆合谱系定位已售产品与责任主体。平台与商家只读取职责内数据，监管角色可查看处置覆盖缺口；消费者凭产品信息查询当前风险、处理方式及依据。

相同事件标识的业务幂等、冲突隔离和状态推进由上层服务负责；本仓库只定义可稳定交换的基础事实。
