# Copyright 2024 Bytedance Ltd. and/or its affiliates
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
DataProtoFactory: DataProto 智能工厂，自动判断使用 LazyDataProto 还是 DataProto。

设计理念：
1. 对上层代码完全透明，无需关心底层实现
2. 根据数据大小、TQ 配置自动选择最优方案
3. 支持多种输入格式（DataProto、Dict、KVBatchMeta）

使用示例：
    from roll.distributed.scheduler.factory import DataProtoFactory
    
    # 从字典创建
    data = DataProtoFactory.create(
        {"tokens": torch.randn(1000, 1024)},
        tq_partition_id="worker_0"
    )
    
    # 从 DataProto 创建
    proto = DataProto(batch=...)
    data = DataProtoFactory.create(proto, tq_partition_id="worker_0")
    
    # 从 KVBatchMeta 创建（直接返回 LazyDataProto）
    data = DataProtoFactory.create(kv_meta, eager_fields=["uid"])
"""

from typing import Dict, List, Optional, Union

import torch

from roll.distributed.scheduler.protocol import DataProto
from roll.distributed.scheduler.lazy_protocol import (
    LazyDataProto,
    DEFAULT_SIZE_THRESHOLD,
)

try:
    from transfer_queue import KVBatchMeta
except ImportError:
    KVBatchMeta = None


class DataProtoFactory:
    """DataProto 智能工厂，自动判断使用 LazyDataProto 还是 DataProto。
    
    设计理念：
    1. 对上层代码完全透明，无需关心底层实现
    2. 根据数据大小、TQ 配置自动选择最优方案
    3. 支持多种输入格式（DataProto、Dict、KVBatchMeta）
    
    使用示例：
        # 从字典创建
        data = DataProtoFactory.create(
            {"tokens": torch.randn(1000, 1024)},
            tq_partition_id="worker_0"
        )
        
        # 从 DataProto 创建
        proto = DataProto(batch=...)
        data = DataProtoFactory.create(proto, tq_partition_id="worker_0")
        
        # 从 KVBatchMeta 创建（直接返回 LazyDataProto）
        data = DataProtoFactory.create(kv_meta, eager_fields=["uid"])
    """

    @staticmethod
    def create(
        data,
        tq_partition_id: Optional[str] = None,
        size_threshold: int = DEFAULT_SIZE_THRESHOLD,
        eager_fields: Optional[list[str]] = None,
        **kwargs
    ) -> DataProto:
        """智能创建 DataProto 实例。

        根据输入数据类型和配置，自动选择最优的实现：
        - 如果输入是 KVBatchMeta，直接创建 LazyDataProto
        - 如果输入是 DataProto，根据数据大小决定是否使用 TQ
        - 如果输入是 Dict，先创建 DataProto，再根据大小决定

        Args:
            data: 输入数据，支持以下类型：
                - KVBatchMeta: TQ 元数据，直接创建 LazyDataProto
                - DataProto: 已有的 DataProto 实例
                - Dict[str, Union[torch.Tensor, np.ndarray]]: 字典格式的数据
                - None: 返回空的 DataProto
            tq_partition_id: TQ 分区 ID，如果为 None 则不使用 TQ
            size_threshold: 数据量阈值（字节），超过此阈值使用 TQ
            eager_fields: 需要立即拉取的字段（仅用于 KVBatchMeta）
            **kwargs: 额外参数，传递给底层构造函数

        Returns:
            DataProto 或 LazyDataProto 实例

        Examples:
            >>> # 从字典创建，大数据自动使用 TQ
            >>> data = DataProtoFactory.create(
            ...     {"tokens": torch.randn(10000, 1024)},
            ...     tq_partition_id="worker_0"
            ... )
            
            >>> # 从 KVBatchMeta 创建
            >>> data = DataProtoFactory.create(kv_meta, eager_fields=["uid"])
            
            >>> # 小数据直接返回 DataProto
            >>> data = DataProtoFactory.create(
            ...     {"labels": torch.randint(0, 10, (100,))},
            ...     tq_partition_id="worker_0"
            ... )
        """
        if KVBatchMeta is not None and isinstance(data, KVBatchMeta):
            return DataProtoFactory._from_kv_meta(
                data, eager_fields=eager_fields
            )

        if isinstance(data, DataProto):
            return DataProtoFactory._from_data_proto(
                data, tq_partition_id=tq_partition_id, size_threshold=size_threshold
            )

        if isinstance(data, dict):
            return DataProtoFactory._from_dict(
                data, tq_partition_id=tq_partition_id, size_threshold=size_threshold, **kwargs
            )

        if data is None:
            return DataProto(batch=None, non_tensor_batch={}, meta_info=kwargs.get("meta_info", {}))

        raise TypeError(
            f"Unsupported data type: {type(data)}. "
            f"Expected KVBatchMeta, DataProto, Dict, or None."
        )

    @staticmethod
    def _from_kv_meta(
        kv_meta: KVBatchMeta,
        eager_fields: Optional[list[str]] = None,
    ) -> LazyDataProto:
        """从 KVBatchMeta 创建 LazyDataProto。

        Args:
            kv_meta: TQ 中的 KV 元数据
            eager_fields: 需要立即拉取的字段

        Returns:
            LazyDataProto 实例
        """
        return LazyDataProto.from_kv_batch_meta(
            kv_meta, fields=kv_meta.fields, eager_fields=eager_fields
        )

    @staticmethod
    def _from_data_proto(
        data_proto: DataProto,
        tq_partition_id: Optional[str] = None,
        size_threshold: int = DEFAULT_SIZE_THRESHOLD,
    ) -> DataProto:
        """从 DataProto 创建，自动判断是否使用 TQ。

        Args:
            data_proto: 原始 DataProto
            tq_partition_id: TQ 分区 ID
            size_threshold: 数据量阈值

        Returns:
            如果不需要 TQ，返回原始 DataProto；否则返回 LazyDataProto
        """
        return LazyDataProto.from_data_proto(
            data_proto, tq_partition_id=tq_partition_id, size_threshold=size_threshold
        )

    @staticmethod
    def _from_dict(
        data: dict,
        tq_partition_id: Optional[str] = None,
        size_threshold: int = DEFAULT_SIZE_THRESHOLD,
        meta_info: Optional[dict] = None,
        **kwargs
    ) -> DataProto:
        """从字典创建，自动判断是否使用 TQ。

        Args:
            data: 字典格式的数据
            tq_partition_id: TQ 分区 ID
            size_threshold: 数据量阈值
            meta_info: 元信息

        Returns:
            DataProto 或 LazyDataProto 实例
        """
        data_proto = DataProto.from_single_dict(data, meta_info=meta_info)
        return DataProtoFactory._from_data_proto(
            data_proto, tq_partition_id=tq_partition_id, size_threshold=size_threshold
        )

    @staticmethod
    def should_use_tq(
        data: Union[DataProto, dict],
        size_threshold: int = DEFAULT_SIZE_THRESHOLD,
    ) -> bool:
        """判断是否应该使用 TQ。

        这是一个辅助方法，用于在创建前判断数据是否会触发 TQ。

        Args:
            data: DataProto 或字典数据
            size_threshold: 数据量阈值

        Returns:
            True 表示应该使用 TQ，False 表示不需要

        Examples:
            >>> data = {"tokens": torch.randn(10000, 1024)}
            >>> if DataProtoFactory.should_use_tq(data):
            ...     print("Will use TQ")
        """
        if isinstance(data, dict):
            temp_proto = DataProto.from_single_dict(data)
            size = LazyDataProto._estimate_data_size(temp_proto)
        elif isinstance(data, DataProto):
            size = LazyDataProto._estimate_data_size(data)
        else:
            raise TypeError(f"Unsupported type: {type(data)}")

        return size >= size_threshold

    @staticmethod
    def estimate_size(data: Union[DataProto, dict]) -> int:
        """估算数据大小（字节）。

        Args:
            data: DataProto 或字典数据

        Returns:
            估算的字节数

        Examples:
            >>> data = {"tokens": torch.randn(100, 100)}
            >>> size = DataProtoFactory.estimate_size(data)
            >>> print(f"Data size: {size / 1024 / 1024:.2f} MB")
        """
        if isinstance(data, dict):
            temp_proto = DataProto.from_single_dict(data)
            return LazyDataProto._estimate_data_size(temp_proto)
        elif isinstance(data, DataProto):
            return LazyDataProto._estimate_data_size(data)
        else:
            raise TypeError(f"Unsupported type: {type(data)}")

    @staticmethod
    def create_empty(
        tq_partition_id: Optional[str] = None,
        meta_info: Optional[Dict] = None,
    ) -> DataProto:
        """创建空的 DataProto 实例。

        如果 tq_partition_id 不为 None，创建 LazyDataProto；
        否则创建普通 DataProto。

        Args:
            tq_partition_id: TQ 分区 ID，如果为 None 则不使用 TQ
            meta_info: 元信息

        Returns:
            DataProto 或 LazyDataProto 实例

        Examples:
            >>> # 创建普通 DataProto
            >>> batch = DataProtoFactory.create_empty()

            >>> # 创建 LazyDataProto
            >>> batch = DataProtoFactory.create_empty(
            ...     tq_partition_id="worker_0",
            ...     meta_info={"global_step": 1}
            ... )
        """
        if tq_partition_id is not None:
            return LazyDataProto(
                batch=None,
                non_tensor_batch={},
                meta_info=meta_info or {},
            )

        return DataProto(
            batch=None,
            non_tensor_batch={},
            meta_info=meta_info or {},
        )

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
        
        merged = DataProto.concat(batches, global_keys=global_keys)
        
        if tq_partition_id is not None:
            return DataProtoFactory._from_data_proto(
                merged,
                tq_partition_id=tq_partition_id,
                size_threshold=DEFAULT_SIZE_THRESHOLD,
            )
        
        return merged
