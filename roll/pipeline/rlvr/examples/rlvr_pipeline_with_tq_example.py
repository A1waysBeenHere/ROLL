"""
RLVRPipelineWithTQ 使用示例

展示如何使用新的 pipeline，通过 tq_partition_id 参数控制是否使用 TransferQueue。
"""

from roll.pipeline.rlvr.rlvr_pipeline_with_tq import RLVRPipelineWithTQ
from roll.pipeline.rlvr.rlvr_config import RLVRConfig


def example_1_without_tq():
    """示例 1: 不使用 TransferQueue（传统模式）
    
    传入 tq_partition_id=None，所有数据都在内存中处理
    """
    print("\n=== 示例 1: 不使用 TransferQueue ===")
    
    config = RLVRConfig.from_file("path/to/config.yaml")
    
    pipeline = RLVRPipelineWithTQ(
        pipeline_config=config,
        tq_partition_id=None,  # 不使用 TQ
    )
    
    pipeline.run()


def example_2_with_tq():
    """示例 2: 使用 TransferQueue（大数据优化模式）
    
    传入 tq_partition_id，大数据自动使用 TransferQueue
    """
    print("\n=== 示例 2: 使用 TransferQueue ===")
    
    config = RLVRConfig.from_file("path/to/config.yaml")
    
    pipeline = RLVRPipelineWithTQ(
        pipeline_config=config,
        tq_partition_id="worker_0",  # 使用 TQ，partition ID 为 worker_0
    )
    
    pipeline.run()


def example_3_multiple_workers():
    """示例 3: 多个 worker 使用不同的 partition
    
    每个 worker 使用独立的 partition，避免数据混淆
    """
    print("\n=== 示例 3: 多个 worker 使用不同的 partition ===")
    
    config = RLVRConfig.from_file("path/to/config.yaml")
    
    worker_id = "worker_0"
    
    pipeline = RLVRPipelineWithTQ(
        pipeline_config=config,
        tq_partition_id=f"rlvr_{worker_id}",  # 每个 worker 独立的 partition
    )
    
    pipeline.run()


def example_4_dynamic_control():
    """示例 4: 动态控制是否使用 TQ
    
    根据环境变量或配置动态决定是否使用 TQ
    """
    print("\n=== 示例 4: 动态控制 ===")
    
    import os
    
    config = RLVRConfig.from_file("path/to/config.yaml")
    
    # 从环境变量读取配置
    use_tq = os.getenv("USE_TRANSFER_QUEUE", "false").lower() == "true"
    tq_partition_id = os.getenv("TQ_PARTITION_ID", "worker_0")
    
    pipeline = RLVRPipelineWithTQ(
        pipeline_config=config,
        tq_partition_id=tq_partition_id if use_tq else None,
    )
    
    pipeline.run()


def example_5_backward_compatible():
    """示例 5: 向后兼容原始 RLVRPipeline
    
    如果不传 tq_partition_id，行为与原始 pipeline 完全一致
    """
    print("\n=== 示例 5: 向后兼容 ===")
    
    config = RLVRConfig.from_file("path/to/config.yaml")
    
    # 方式 1: 使用新 pipeline，不传 tq_partition_id
    pipeline = RLVRPipelineWithTQ(
        pipeline_config=config,
        # tq_partition_id 默认为 None
    )
    
    # 方式 2: 使用原始 pipeline（完全相同的行为）
    from roll.pipeline.rlvr.rlvr_pipeline import RLVRPipeline
    original_pipeline = RLVRPipeline(pipeline_config=config)
    
    # 两者行为完全一致


if __name__ == "__main__":
    print("=" * 60)
    print("RLVRPipelineWithTQ 使用示例")
    print("=" * 60)
    
    print("\n核心改动:")
    print("1. 构造函数新增参数: tq_partition_id: Optional[str] = None")
    print("2. 使用 DataProtoFactory.create() 替代 DataProto() 构造")
    print("3. 其他代码完全不变，保持向后兼容")
    
    print("\n使用方式:")
    print("```python")
    print("from roll.pipeline.rlvr.rlvr_pipeline_with_tq import RLVRPipelineWithTQ")
    print("")
    print("# 不使用 TQ（传统模式）")
    print("pipeline = RLVRPipelineWithTQ(config, tq_partition_id=None)")
    print("")
    print("# 使用 TQ（大数据优化）")
    print("pipeline = RLVRPipelineWithTQ(config, tq_partition_id='worker_0')")
    print("```")
    
    print("\n" + "=" * 60)
