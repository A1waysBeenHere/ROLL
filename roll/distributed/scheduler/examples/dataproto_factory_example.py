"""
DataProtoFactory 使用示例

展示如何使用 DataProtoFactory 自动判断使用 LazyDataProto 还是 DataProto。
"""

import torch
import numpy as np
from roll.distributed.scheduler.factory import DataProtoFactory
from roll.distributed.scheduler.lazy_protocol import LazyDataProto
from roll.distributed.scheduler.protocol import DataProto


def example_1_from_dict_small_data():
    """示例 1: 从字典创建小数据，自动使用 DataProto"""
    print("\n=== 示例 1: 小数据自动使用 DataProto ===")
    
    data_dict = {
        "labels": torch.randint(0, 10, (100,)),  # 小数据
        "scores": torch.randn(100),
    }
    
    proto = DataProtoFactory.create(
        data_dict,
        tq_partition_id="worker_0",  # 即使指定了 partition
        size_threshold=100 * 1024 * 1024,  # 100MB 阈值
    )
    
    print(f"类型: {type(proto).__name__}")
    print(f"是否为 LazyDataProto: {isinstance(proto, LazyDataProto)}")
    print(f"数据大小: {DataProtoFactory.estimate_size(data_dict) / 1024:.2f} KB")


def example_2_from_dict_large_data():
    """示例 2: 从字典创建大数据，自动使用 LazyDataProto"""
    print("\n=== 示例 2: 大数据自动使用 LazyDataProto ===")
    
    data_dict = {
        "tokens": torch.randn(10000, 1024),  # 大数据 (~40MB)
        "attention_mask": torch.ones(10000, 1024),
    }
    
    proto = DataProtoFactory.create(
        data_dict,
        tq_partition_id="worker_0",
        size_threshold=10 * 1024 * 1024,  # 10MB 阈值（降低阈值以便演示）
    )
    
    print(f"类型: {type(proto).__name__}")
    print(f"是否为 LazyDataProto: {isinstance(proto, LazyDataProto)}")
    print(f"数据大小: {DataProtoFactory.estimate_size(data_dict) / 1024 / 1024:.2f} MB")
    
    if isinstance(proto, LazyDataProto):
        print(f"是否惰性模式: {proto.is_lazy}")


def example_3_from_dataproto():
    """示例 3: 从 DataProto 创建"""
    print("\n=== 示例 3: 从 DataProto 创建 ===")
    
    original_proto = DataProto.from_single_dict({
        "input_ids": torch.randint(0, 1000, (5000, 512)),
        "attention_mask": torch.ones(5000, 512),
    })
    
    print(f"原始类型: {type(original_proto).__name__}")
    
    # 自动判断是否使用 TQ
    proto = DataProtoFactory.create(
        original_proto,
        tq_partition_id="worker_0",
        size_threshold=10 * 1024 * 1024,
    )
    
    print(f"转换后类型: {type(proto).__name__}")
    print(f"是否为 LazyDataProto: {isinstance(proto, LazyDataProto)}")


def example_4_from_kv_meta():
    """示例 4: 从 KVBatchMeta 创建（直接返回 LazyDataProto）"""
    print("\n=== 示例 4: 从 KVBatchMeta 创建 ===")
    
    try:
        from transfer_queue import KVBatchMeta
        
        kv_meta = KVBatchMeta(
            partition_id="worker_0",
            keys=["key_0", "key_1", "key_2"],
            tags=[{}, {}, {}],
            fields=["tokens", "labels"],
        )
        
        proto = DataProtoFactory.create(
            kv_meta,
            eager_fields=["labels"],  # 立即拉取 labels 字段
        )
        
        print(f"类型: {type(proto).__name__}")
        print(f"是否为 LazyDataProto: {isinstance(proto, LazyDataProto)}")
        print(f"是否惰性模式: {proto.is_lazy}")
        
    except ImportError:
        print("TransferQueue 未安装，跳过此示例")


def example_5_check_before_create():
    """示例 5: 创建前检查是否使用 TQ"""
    print("\n=== 示例 5: 创建前检查 ===")
    
    data_dict = {
        "features": torch.randn(5000, 768),
    }
    
    if DataProtoFactory.should_use_tq(data_dict, size_threshold=10 * 1024 * 1024):
        print("数据较大，将使用 LazyDataProto")
        print(f"数据大小: {DataProtoFactory.estimate_size(data_dict) / 1024 / 1024:.2f} MB")
    else:
        print("数据较小，将使用普通 DataProto")


def example_6_none_input():
    """示例 6: 输入 None 创建空 DataProto"""
    print("\n=== 示例 6: 创建空 DataProto ===")
    
    proto = DataProtoFactory.create(
        None,
        meta_info={"description": "Empty proto"},
    )
    
    print(f"类型: {type(proto).__name__}")
    print(f"batch: {proto.batch}")
    print(f"meta_info: {proto.meta_info}")


def example_7_transparent_usage():
    """示例 7: 透明使用，无需关心底层实现"""
    print("\n=== 示例 7: 透明使用 ===")
    
    def process_data(data, tq_partition_id=None):
        """处理数据的函数，无需关心数据是 DataProto 还是 LazyDataProto"""
        proto = DataProtoFactory.create(data, tq_partition_id=tq_partition_id)
        
        print(f"  类型: {type(proto).__name__}")
        print(f"  长度: {len(proto)}")
        
        if proto.batch is not None:
            print(f"  batch keys: {list(proto.batch.keys())}")
        
        return proto
    
    # 小数据
    small_data = {"x": torch.randn(100, 10)}
    print("处理小数据:")
    process_data(small_data, tq_partition_id="worker_0")
    
    # 大数据
    large_data = {"x": torch.randn(10000, 1000)}
    print("\n处理大数据:")
    process_data(large_data, tq_partition_id="worker_0")


def example_8_custom_threshold():
    """示例 8: 自定义阈值"""
    print("\n=== 示例 8: 自定义阈值 ===")
    
    data_dict = {
        "features": torch.randn(1000, 512),  # ~2MB
    }
    
    # 使用默认阈值（100MB），不会触发 TQ
    proto1 = DataProtoFactory.create(
        data_dict,
        tq_partition_id="worker_0",
    )
    print(f"默认阈值 (100MB): {type(proto1).__name__}")
    
    # 降低阈值到 1MB，会触发 TQ
    proto2 = DataProtoFactory.create(
        data_dict,
        tq_partition_id="worker_0",
        size_threshold=1 * 1024 * 1024,  # 1MB
    )
    print(f"降低阈值 (1MB): {type(proto2).__name__}")


if __name__ == "__main__":
    print("=" * 60)
    print("DataProtoFactory 使用示例")
    print("=" * 60)
    
    example_1_from_dict_small_data()
    example_2_from_dict_large_data()
    example_3_from_dataproto()
    example_4_from_kv_meta()
    example_5_check_before_create()
    example_6_none_input()
    example_7_transparent_usage()
    example_8_custom_threshold()
    
    print("\n" + "=" * 60)
    print("所有示例完成！")
    print("=" * 60)
