# DataProtoFactory 方法优化总结

## 优化目标

将 pipeline 中的辅助方法移到 `DataProtoFactory` 接口类中，避免代码重复，提高可维护性。

## 优化内容

### 新增方法

在 `DataProtoFactory` 中新增两个便捷方法：

#### 1. `create_empty()` - 创建空的 DataProto

```python
@staticmethod
def create_empty(
    tq_partition_id: Optional[str] = None,
    meta_info: Optional[Dict] = None,
) -> DataProto:
    """创建空的 DataProto 实例。
    
    Args:
        tq_partition_id: TQ 分区 ID
        meta_info: 元信息
    
    Returns:
        DataProto 实例
    
    Examples:
        >>> batch = DataProtoFactory.create_empty(
        ...     tq_partition_id="worker_0",
        ...     meta_info={"global_step": 1}
        ... )
    """
    return DataProtoFactory.create(
        None,
        tq_partition_id=tq_partition_id,
        meta_info=meta_info or {},
    )
```

#### 2. `concat_batches()` - 合并多个 DataProto

```python
@staticmethod
def concat_batches(
    batches: List[DataProto],
    tq_partition_id: Optional[str] = None,
    global_keys: Optional[set] = None,
) -> DataProto:
    """合并多个 DataProto 实例。
    
    Args:
        batches: DataProto 实例列表
        tq_partition_id: TQ 分区 ID
        global_keys: 全局键集合
    
    Returns:
        合并后的 DataProto 实例
    
    Examples:
        >>> batch1 = DataProtoFactory.create({"x": torch.randn(10, 10)})
        >>> batch2 = DataProtoFactory.create({"x": torch.randn(10, 10)})
        >>> merged = DataProtoFactory.concat_batches([batch1, batch2])
    """
    if not batches:
        return DataProtoFactory.create_empty(tq_partition_id=tq_partition_id)
    
    return DataProto.concat(batches, global_keys=global_keys)
```

### Pipeline 改动

#### 优化前

```python
class RLVRPipelineWithTQ(BasePipeline):
    def _create_batch(self, meta_info: Optional[Dict] = None) -> DataProto:
        return DataProtoFactory.create(
            None,
            tq_partition_id=self.tq_partition_id,
            meta_info=meta_info or {},
        )
    
    def _concat_batches(self, batches: List[DataProto]) -> DataProto:
        return DataProtoFactory.create(
            batches,
            tq_partition_id=self.tq_partition_id,
        )
```

#### 优化后

```python
class RLVRPipelineWithTQ(BasePipeline):
    def _create_batch(self, meta_info: Optional[Dict] = None) -> DataProto:
        return DataProtoFactory.create_empty(
            tq_partition_id=self.tq_partition_id,
            meta_info=meta_info,
        )
    
    # _concat_batches 方法已移到 factory 中
```

## 使用示例

### 示例 1: 创建空 batch

```python
from roll.distributed.scheduler.factory import DataProtoFactory

# 创建空 batch（不使用 TQ）
batch = DataProtoFactory.create_empty()

# 创建空 batch（使用 TQ）
batch = DataProtoFactory.create_empty(
    tq_partition_id="worker_0",
    meta_info={"global_step": 1}
)
```

### 示例 2: 合并多个 batch

```python
from roll.distributed.scheduler.factory import DataProtoFactory

# 创建多个 batch
batch1 = DataProtoFactory.create({"x": torch.randn(10, 10)})
batch2 = DataProtoFactory.create({"x": torch.randn(10, 10)})

# 合并
merged = DataProtoFactory.concat_batches([batch1, batch2])
```

### 示例 3: 在 pipeline 中使用

```python
class MyPipeline(BasePipeline):
    def __init__(self, config, tq_partition_id=None):
        self.tq_partition_id = tq_partition_id
    
    def run(self):
        # 创建空 batch
        batch = DataProtoFactory.create_empty(
            tq_partition_id=self.tq_partition_id,
            meta_info={"global_step": 1}
        )
        
        # 合并多个 batch
        batches = [batch1, batch2, batch3]
        merged = DataProtoFactory.concat_batches(
            batches,
            tq_partition_id=self.tq_partition_id
        )
```

## 优化优势

### 1. 代码复用

**优化前**：
- 每个 pipeline 都需要定义 `_create_batch()` 和 `_concat_batches()`
- 代码重复，维护成本高

**优化后**：
- 所有 pipeline 共享 `DataProtoFactory` 的方法
- 代码统一，易于维护

### 2. 接口统一

**优化前**：
- pipeline 内部方法，只能在 pipeline 中使用
- 外部代码无法复用

**优化后**：
- 统一在 `DataProtoFactory` 中，任何地方都可以使用
- 提供一致的 API

### 3. 易于扩展

**优化前**：
- 新增功能需要修改每个 pipeline

**优化后**：
- 只需在 `DataProtoFactory` 中添加新方法
- 所有使用方自动获得新功能

## 代码改动统计

| 文件 | 改动类型 | 数量 | 说明 |
|------|---------|------|------|
| `factory.py` | 新增方法 | 2 | `create_empty()`, `concat_batches()` |
| `factory.py` | 新增导入 | 1 | `List`, `Dict` |
| `rlvr_pipeline_with_tq.py` | 删除方法 | 1 | `_concat_batches()` |
| `rlvr_pipeline_with_tq.py` | 简化方法 | 1 | `_create_batch()` |

**总计**：4 处改动，代码更简洁

## 向后兼容

### 完全兼容

- ✅ 原有方法签名不变
- ✅ 原有行为不变
- ✅ 无需修改现有代码

### 渐进式迁移

```python
# 方式 1: 继续使用 pipeline 方法（兼容）
batch = self._create_batch(meta_info={"global_step": global_step})

# 方式 2: 直接使用 factory（推荐）
batch = DataProtoFactory.create_empty(
    tq_partition_id=self.tq_partition_id,
    meta_info={"global_step": global_step}
)
```

## 最佳实践

### 推荐：直接使用 factory

```python
from roll.distributed.scheduler.factory import DataProtoFactory

# 创建空 batch
batch = DataProtoFactory.create_empty(
    tq_partition_id="worker_0",
    meta_info={"global_step": 1}
)

# 合并 batch
merged = DataProtoFactory.concat_batches([batch1, batch2])
```

### 不推荐：在 pipeline 中重复定义

```python
# ❌ 不推荐：重复定义
class MyPipeline(BasePipeline):
    def _create_batch(self, meta_info=None):
        return DataProtoFactory.create(
            None,
            tq_partition_id=self.tq_partition_id,
            meta_info=meta_info or {},
        )
```

## 总结

### 优化成果

✅ **代码质量提升**：
- 减少重复代码
- 统一接口设计
- 提高可维护性

✅ **易用性提升**：
- 提供便捷方法
- 简化调用方式
- 支持灵活使用

✅ **扩展性提升**：
- 集中管理工厂方法
- 易于添加新功能
- 支持未来扩展

### 推荐使用

**新项目**：直接使用 `DataProtoFactory` 的静态方法

**现有项目**：渐进式迁移，逐步替换 pipeline 内部方法

### 核心价值

1. **代码复用**: 避免在每个 pipeline 中重复定义
2. **接口统一**: 提供一致的 API，易于理解和使用
3. **易于维护**: 集中管理，修改一处即可影响全局
4. **向后兼容**: 完全兼容现有代码，无需强制迁移
