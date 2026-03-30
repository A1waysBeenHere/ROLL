# RLVRPipelineWithTQ 使用指南

## 概述

`RLVRPipelineWithTQ` 是 `RLVRPipeline` 的增强版本，集成了 `DataProtoFactory` 接口层，支持 TransferQueue (TQ) 优化。

## 核心改动

### 1. 构造函数新增参数

```python
# 原始版本
class RLVRPipeline(BasePipeline):
    def __init__(self, pipeline_config: RLVRConfig):
        ...

# 新版本
class RLVRPipelineWithTQ(BasePipeline):
    def __init__(self, pipeline_config: RLVRConfig, tq_partition_id: Optional[str] = None):
        ...
```

**参数说明**：
- `tq_partition_id`: TransferQueue 分区 ID
  - `None` (默认): 不使用 TQ，所有数据在内存中处理（与原始 pipeline 行为一致）
  - `"worker_0"` 等: 使用 TQ，大数据自动存储到 TransferQueue

### 2. 使用 DataProtoFactory

**原始代码**：
```python
batch = DataProto(meta_info={"global_step": global_step})
```

**新代码**：
```python
batch = self._create_batch(meta_info={"global_step": global_step})

# 辅助方法
def _create_batch(self, meta_info: Optional[Dict] = None) -> DataProto:
    return DataProtoFactory.create(
        None,
        tq_partition_id=self.tq_partition_id,
        meta_info=meta_info or {},
    )
```

### 3. 其他改动

- 新增 `_create_batch()` 辅助方法
- 新增 `_concat_batches()` 辅助方法（预留）
- 其他代码完全不变，保持向后兼容

## 使用方式

### 方式 1: 不使用 TransferQueue（默认）

```python
from roll.pipeline.rlvr.rlvr_pipeline_with_tq import RLVRPipelineWithTQ
from roll.pipeline.rlvr.rlvr_config import RLVRConfig

config = RLVRConfig.from_file("config.yaml")

# 不传 tq_partition_id，行为与原始 pipeline 完全一致
pipeline = RLVRPipelineWithTQ(config)
pipeline.run()
```

### 方式 2: 使用 TransferQueue

```python
from roll.pipeline.rlvr.rlvr_pipeline_with_tq import RLVRPipelineWithTQ
from roll.pipeline.rlvr.rlvr_config import RLVRConfig

config = RLVRConfig.from_file("config.yaml")

# 传入 tq_partition_id，启用 TQ 优化
pipeline = RLVRPipelineWithTQ(
    config,
    tq_partition_id="worker_0"
)
pipeline.run()
```

### 方式 3: 动态控制

```python
import os
from roll.pipeline.rlvr.rlvr_pipeline_with_tq import RLVRPipelineWithTQ

config = RLVRConfig.from_file("config.yaml")

# 根据环境变量动态决定
use_tq = os.getenv("USE_TRANSFER_QUEUE", "false").lower() == "true"
tq_partition_id = os.getenv("TQ_PARTITION_ID", "worker_0") if use_tq else None

pipeline = RLVRPipelineWithTQ(
    config,
    tq_partition_id=tq_partition_id
)
pipeline.run()
```

## 代码对比

### 原始 pipeline (rlvr_pipeline.py)

```python
class RLVRPipeline(BasePipeline):
    def __init__(self, pipeline_config: RLVRConfig):
        super().__init__(pipeline_config)
        # ... 初始化代码 ...
    
    def run(self):
        # ...
        batch = DataProto(meta_info={"global_step": global_step})
        # ...
```

### 新 pipeline (rlvr_pipeline_with_tq.py)

```python
class RLVRPipelineWithTQ(BasePipeline):
    def __init__(self, pipeline_config: RLVRConfig, tq_partition_id: Optional[str] = None):
        super().__init__(pipeline_config)
        self.tq_partition_id = tq_partition_id  # 新增
        # ... 初始化代码（完全相同）...
    
    def _create_batch(self, meta_info: Optional[Dict] = None) -> DataProto:
        """新增辅助方法"""
        return DataProtoFactory.create(
            None,
            tq_partition_id=self.tq_partition_id,
            meta_info=meta_info or {},
        )
    
    def run(self):
        # ...
        batch = self._create_batch(meta_info={"global_step": global_step})  # 修改
        # ... 其他代码完全相同 ...
```

## 性能对比

### 不使用 TQ (tq_partition_id=None)

- **内存占用**: 所有数据都在内存中
- **适用场景**: 小规模数据训练
- **行为**: 与原始 `RLVRPipeline` 完全一致

### 使用 TQ (tq_partition_id="worker_0")

- **内存占用**: 大数据自动存储到 TQ，按需拉取
- **适用场景**: 大规模数据训练
- **行为**: 
  - 数据大小 < 100MB: 在内存中处理
  - 数据大小 ≥ 100MB: 存储到 TQ，惰性加载

## 迁移指南

### 从 RLVRPipeline 迁移

**步骤 1**: 修改导入

```python
# 旧代码
from roll.pipeline.rlvr.rlvr_pipeline import RLVRPipeline

# 新代码
from roll.pipeline.rlvr.rlvr_pipeline_with_tq import RLVRPipelineWithTQ as RLVRPipeline
```

**步骤 2**: （可选）启用 TQ

```python
# 如果需要 TQ 优化，传入 tq_partition_id
pipeline = RLVRPipeline(config, tq_partition_id="worker_0")

# 如果不需要，保持原样即可
pipeline = RLVRPipeline(config)
```

### 无需修改的场景

以下场景无需修改代码：

1. **配置文件**: 完全兼容，无需修改
2. **Worker 类**: 完全兼容，无需修改
3. **其他依赖**: 完全兼容，无需修改

## 最佳实践

### 1. 根据数据规模选择

```python
# 小数据集（< 10GB）: 不使用 TQ
pipeline = RLVRPipelineWithTQ(config, tq_partition_id=None)

# 大数据集（≥ 10GB）: 使用 TQ
pipeline = RLVRPipelineWithTQ(config, tq_partition_id="worker_0")
```

### 2. 多 worker 场景

```python
# 每个 worker 使用独立的 partition
worker_id = os.getenv("WORKER_ID", "worker_0")
pipeline = RLVRPipelineWithTQ(
    config,
    tq_partition_id=f"rlvr_{worker_id}"
)
```

### 3. 调试模式

```python
# 调试时禁用 TQ，便于观察数据流
if os.getenv("DEBUG_MODE", "false").lower() == "true":
    pipeline = RLVRPipelineWithTQ(config, tq_partition_id=None)
else:
    pipeline = RLVRPipelineWithTQ(config, tq_partition_id="worker_0")
```

## 常见问题

### Q: 是否需要修改配置文件？

**A**: 不需要。配置文件完全兼容，无需任何修改。

### Q: 是否影响现有功能？

**A**: 不影响。当 `tq_partition_id=None` 时，行为与原始 pipeline 完全一致。

### Q: 如何判断是否应该使用 TQ？

**A**: 建议：
- 数据集 < 10GB: 不使用 TQ
- 数据集 ≥ 10GB: 使用 TQ
- 多 worker 并行: 使用 TQ

### Q: TQ 的性能开销如何？

**A**: 
- 小数据（< 100MB）: 无额外开销，直接在内存中处理
- 大数据（≥ 100MB）: 有轻微的序列化/反序列化开销，但大幅降低内存占用

## 总结

| 特性 | RLVRPipeline | RLVRPipelineWithTQ |
|------|--------------|-------------------|
| 向后兼容 | - | ✅ 完全兼容 |
| TQ 支持 | ❌ | ✅ |
| 代码改动 | - | 最小化（仅 2 处） |
| 配置改动 | - | 无需改动 |
| 性能优化 | - | ✅ 大数据优化 |

**推荐**: 直接使用 `RLVRPipelineWithTQ`，根据需要传入 `tq_partition_id` 参数。
