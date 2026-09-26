# 头部护具召回协同链

表达护具认证、批次谱系、平台流通和强制召回之间的领域事件，并在契约之上提供召回协同服务。

## 目录

- `contracts/domain.schema.json`：对象、事件和载荷字段约定。
- `data/sample.json`：可直接校验的联调样例。
- `src/helmet_recall/contracts.py`：基础契约校验。
- `src/helmet_recall/clock.py`：可控时钟，驱动停售、通知、响应与逾期升级。
- `src/helmet_recall/service.py`：召回协同服务（角色分权、批次谱系、幂等消息、去向仲裁、分角色视图、日志恢复）。
- `src/helmet_recall/cli.py`：命令行校验入口。
- `tests/`：信封、时间、版本、事件载荷与协同服务测试。
- `docs/domain.md`：领域对象、事件语义与服务规则。

## 协同服务示例

```python
from datetime import datetime, timedelta, timezone
from helmet_recall import Actor, ManualClock, RecallService, Role

tz = timezone(timedelta(hours=8))
clock = ManualClock(datetime(2026, 9, 26, 9, 0, tzinfo=tz))
service = RecallService(clock, "journal.jsonl")  # 重启后重放日志继续召回

regulator = Actor("reg-1", Role.REGULATOR)
service.register_producer(regulator, "p-1", "某护具厂商")
# ...登记型号、证书、批次、店铺后：
service.assess_risk(regulator, "lot-1", "high", "抽检无有效认证")  # 先冻结未售库存
service.launch_recall(regulator, "rc-1", ["lot-1"], ["refund"])   # 再按谱系定位已售
service.approve_campaign(regulator, "rc-1")
service.notify_consumers(regulator, "rc-1")
service.consumer_query(serial="s1")  # 消费者得到风险、处理方式及其依据
```

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
