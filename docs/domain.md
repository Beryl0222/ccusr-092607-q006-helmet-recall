# 领域约定

表达护具认证、批次谱系、平台流通和强制召回之间的领域事件。

聚合对象包括`helmet_model`、`certification_record`、`product_lot`、`recall_campaign`。事件类型包括`MODEL_REGISTERED`、`CERTIFICATE_VERIFIED`、`TEST_RESULT_RECORDED`、`LOT_FROZEN`、`UNIT_DISPOSED`。所有发生时间都必须携带时区，版本号从 1 开始递增，基础校验不会改写调用方输入。

## 事件载荷

- `CERTIFICATE_VERIFIED`：载荷还需包含 `certificate_ref`, `valid_until`。
- `LOT_FROZEN`：载荷还需包含 `affected_units`, `risk_reason`。
- `UNIT_DISPOSED`：载荷还需包含 `unit_ref`, `disposition`。

相同事件标识的业务幂等、冲突隔离和状态推进由上层服务负责；本仓库只定义可稳定交换的基础事实。
