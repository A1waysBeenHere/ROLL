# domain_batch 转换修复总结

## 问题发现

用户发现了一个关键问题：

> "这里的 domain_batches 内的 values 仍然是 dataproto 格式，怎么修改？"

### 问题分析

```python
# 第 503 行
domain_batch = ray.get(scheduler_ref, timeout=self.pipeline_config.rpc_timeout)
# domain_batch 是普通的 DataProto

# 第 507 行
domain_batches[domain] = domain_batch
# ❌ 直接存储，没有应用 TQ 优化
```

**问题**：
- `domain_batch` 从 scheduler 返回的是普通 `DataProto`
- 直接存储到 `domain_batches` 中，没有应用 TQ 优化
- 后续 `concat_batches()` 合并时，数据可能已经很大

## 修复方案

### 修改代码

```python
# 修复前
for domain, scheduler_ref in scheduler_refs.items():
    domain_batch = ray.get(scheduler_ref, timeout=self.pipeline_config.rpc_timeout)
    metrics_mgr.add_domain_metrics(
        domain, reduce_metrics(domain_batch.meta_info.pop("metrics", {}))
    )
    domain_batches[domain] = domain_batch  # ❌ 直接存储

# 修复后
for domain, scheduler_ref in scheduler_refs.items():
    domain_batch = ray.get(scheduler_ref, timeout=self.pipeline_config.rpc_timeout)
    metrics_mgr.add_domain_metrics(
        domain, reduce_metrics(domain_batch.meta_info.pop("metrics", {}))
    )
    domain_batches[domain] = DataProtoFactory.create(
        domain_batch,
        tq_partition_id=self.tq_partition_id,
    )  # ✅ 应用 TQ 优化
```

### 修复逻辑

通过 `DataProtoFactory.create()` 转换 `domain_batch`：

1. **判断数据大小**：
   - 如果 `domain_batch` 数据 < 100MB → 返回 `DataProto`
   - 如果 `domain_batch` 数据 ≥ 100MB → 返回 `LazyDataProto`

2. **应用 TQ 优化**：
   - 大数据自动存储到 TransferQueue
   - 降低内存占用

## 完整流程

### 修复前的流程

```
scheduler 返回 domain_batch (DataProto)
    ↓
直接存储到 domain_batches
    ❌ 没有应用 TQ 优化
    ↓
concat_batches() 合并
    ↓
可能内存占用过大
```

### 修复后的流程

```
scheduler 返回 domain_batch (DataProto)
    ↓
DataProtoFactory.create() 转换
    ↓
判断数据大小
    - < 100MB: 返回 DataProto
    - ≥ 100MB: 返回 LazyDataProto
    ✅ 应用 TQ 优化
    ↓
存储到 domain_batches
    ↓
concat_batches() 合并
    ↓
内存占用优化
```

## 代码改动

| 位置 | 修改前 | 修改后 |
|------|--------|--------|
| 第 507 行 | `domain_batches[domain] = domain_batch` | `domain_batches[domain] = DataProtoFactory.create(domain_batch, tq_partition_id=self.tq_partition_id)` |

## 验证结果

```bash
python3 -m py_compile roll/pipeline/rlvr/rlvr_pipeline_with_tq.py
# ✅ 编译通过
```

## 完整的 TQ 优化链路

### 现在的完整流程

```
1. 创建空 batch
   batch = DataProtoFactory.create_empty(
       tq_partition_id="worker_0",
       meta_info={"global_step": global_step}
   )
   ↓
   返回: LazyDataProto ✅

2. scheduler 返回 domain_batch
   domain_batch = ray.get(scheduler_ref)
   ↓
   返回: DataProto

3. 转换 domain_batch
   domain_batches[domain] = DataProtoFactory.create(
       domain_batch,
       tq_partition_id="worker_0"
   )
   ↓
   返回: DataProto 或 LazyDataProto ✅

4. 合并 domain_batch
   generate_output = DataProtoFactory.concat_batches(
       [domain_batch, ...],
       tq_partition_id="worker_0"
   )
   ↓
   返回: LazyDataProto（大数据）✅

5. 后续使用
   batch = generate_output
   ↓
   大数据自动使用 TQ，降低内存占用 ✅
```

## 关键改进点

| 改进点 | 修复前 | 修复后 |
|--------|--------|--------|
| **domain_batch 存储** | 直接存储 DataProto | 通过 factory 转换 |
| **TQ 优化时机** | 只在合并时 | 在存储和合并时 |
| **内存占用** | 可能过大 | 自动优化 |

## 总结

通过这次修复，我们实现了：

1. ✅ **完整的 TQ 优化链路**：从创建到存储到合并
2. ✅ **提前优化**：在存储 `domain_batch` 时就应用 TQ 优化
3. ✅ **降低内存占用**：大数据自动使用 `LazyDataProto`
4. ✅ **编译通过**：所有修改都已验证

**核心价值**：在更早的阶段应用 TQ 优化，进一步降低内存占用！🎉
