# DataProtoFactory 使用指南

## 概述

`DataProtoFactory` 是一个智能工厂类，用于自动判断应该使用 `LazyDataProto` 还是 `DataProto`。它对上层代码完全透明，无需关心底层实现细节。

## 设计理念

1. **透明性**：上层代码无需关心底层使用的是 `LazyDataProto` 还是 `DataProto`
2. **智能选择**：根据数据大小、TQ 配置自动选择最优方案
3. **多格式支持**：支持多种输入格式（DataProto、Dict、KVBatchMeta）

## 核心功能

### 1. 智能创建

根据输入数据类型和配置，自动选择最优的实现：

- **KVBatchMeta** → 直接创建 `LazyDataProto`
- **DataProto** → 根据数据大小决定是否使用 TQ
- **Dict** → 先创建 `DataProto`，再根据大小决定

### 2. 自动判断

- 数据大小 < 阈值 → 使用 `DataProto`（内存存储）
- 数据大小 ≥ 阈值 → 使用 `LazyDataProto`（TQ 存储）

## 使用示例

### 基础用法

```python
from roll.distributed.scheduler.factory import DataProtoFactory
import torch

# 从字典创建，自动判断
data = DataProtoFactory.create(
    {"tokens": torch.randn(10000, 1024)},
    tq_partition_id="worker_0"
)
```

### 从不同数据源创建

#### 1. 从字典创建

```python
# 小数据 → DataProto
small_data = DataProtoFactory.create(
    {"labels": torch.randint(0, 10, (100,))},
    tq_partition_id="worker_0"
)
# 类型: DataProto

# 大数据 → LazyDataProto
large_data = DataProtoFactory.create(
    {"tokens": torch.randn(10000, 1024)},
    tq_partition_id="worker_0",
    size_threshold=10 * 1024 * 1024  # 10MB
)
# 类型: LazyDataProto
```

#### 2. 从 DataProto 创建

```python
from roll.distributed.scheduler.protocol import DataProto

proto = DataProto.from_single_dict({
    "input_ids": torch.randint(0, 1000, (5000, 512)),
})

# 自动判断是否使用 TQ
data = DataProtoFactory.create(
    proto,
    tq_partition_id="worker_0"
)
```

#### 3. 从 KVBatchMeta 创建

```python
from transfer_queue import KVBatchMeta

kv_meta = KVBatchMeta(
    partition_id="worker_0",
    keys=["key_0", "key_1", "key_2"],
    tags=[{}, {}, {}],
    fields=["tokens", "labels"],
)

# 直接创建 LazyDataProto
data = DataProtoFactory.create(
    kv_meta,
    eager_fields=["labels"]  # 立即拉取 labels 字段
)
```

### 辅助方法

#### 判断是否使用 TQ

```python
data_dict = {"features": torch.randn(5000, 768)}

if DataProtoFactory.should_use_tq(data_dict, size_threshold=10 * 1024 * 1024):
    print("将使用 LazyDataProto")
else:
    print("将使用普通 DataProto")
```

#### 估算数据大小

```python
data_dict = {"tokens": torch.randn(100, 100)}
size = DataProtoFactory.estimate_size(data_dict)
print(f"数据大小: {size / 1024 / 1024:.2f} MB")
```

## 参数说明

### `create()` 方法

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `data` | Any | 必需 | 输入数据（KVBatchMeta/DataProto/Dict/None） |
| `tq_partition_id` | str \| None | None | TQ 分区 ID，为 None 则不使用 TQ |
| `size_threshold` | int | 100MB | 数据量阈值，超过则使用 TQ |
| `eager_fields` | list[str] \| None | None | 需要立即拉取的字段（仅用于 KVBatchMeta） |
| `**kwargs` | dict | - | 额外参数，传递给底层构造函数 |

### `should_use_tq()` 方法

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `data` | DataProto \| dict | 必需 | 要检查的数据 |
| `size_threshold` | int | 100MB | 数据量阈值 |

### `estimate_size()` 方法

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `data` | DataProto \| dict | 必需 | 要估算的数据 |

## 最佳实践

### 1. 使用默认阈值

```python
# 推荐：使用默认阈值（100MB）
data = DataProtoFactory.create(
    {"tokens": torch.randn(10000, 1024)},
    tq_partition_id="worker_0"
)
```

### 2. 透明使用

```python
def process_data(data, tq_partition_id=None):
    """处理数据的函数，无需关心底层实现"""
    proto = DataProtoFactory.create(data, tq_partition_id=tq_partition_id)
    
    # 统一的操作接口
    print(f"长度: {len(proto)}")
    if proto.batch is not None:
        print(f"batch keys: {list(proto.batch.keys())}")
    
    return proto
```

### 3. 自定义阈值

```python
# 根据实际需求调整阈值
data = DataProtoFactory.create(
    large_data,
    tq_partition_id="worker_0",
    size_threshold=50 * 1024 * 1024  # 50MB
)
```

## 性能优化建议

1. **合理设置阈值**：根据可用内存和 TQ 性能调整 `size_threshold`
2. **使用 eager_fields**：对于需要频繁访问的小字段，使用 `eager_fields` 立即拉取
3. **避免频繁创建**：尽量复用已创建的 DataProto 实例

## 常见问题

### Q: 如何知道返回的是哪种类型？

```python
data = DataProtoFactory.create(...)

if isinstance(data, LazyDataProto):
    print("使用 LazyDataProto")
    print(f"是否惰性模式: {data.is_lazy}")
else:
    print("使用普通 DataProto")
```

### Q: 如何强制使用 TQ？

```python
# 方法 1: 降低阈值
data = DataProtoFactory.create(
    data_dict,
    tq_partition_id="worker_0",
    size_threshold=0  # 强制使用 TQ
)

# 方法 2: 直接创建 LazyDataProto
from roll.distributed.scheduler.lazy_protocol import LazyDataProto
data = LazyDataProto.from_data_proto(
    DataProto.from_single_dict(data_dict),
    tq_partition_id="worker_0"
)
```

### Q: 如何禁用 TQ？

```python
# 不传递 tq_partition_id
data = DataProtoFactory.create(data_dict)  # 总是返回 DataProto
```

## 相关文档

- [LazyDataProto 文档](./lazy_protocol.py)
- [DataProto 文档](./protocol.py)
- [完整示例](./examples/dataproto_factory_example.py)
