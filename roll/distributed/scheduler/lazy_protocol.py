
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
LazyDataProto: DataProto 的 TransferQueue 透明代理。

数据实际存储在 TransferQueue 中，通过 KVBatchMeta 作为"指针"。
当用户访问 batch 或 non_tensor_batch 时，自动从 TQ 拉取数据（materialize）；
chunk/concat/select/reorder 等操作在元数据层面完成，不触发数据拉取。

设计原则：
1. 继承 DataProto，所有 DataProto 的操作都兼容
2. tq_client 完全内部维护，用户不感知
3. 自动判断是否需要 TQ（小数据直接走内存）
4. materialize/dematerialize 对用户透明
"""

import copy
import logging
import os
import threading
import uuid
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import torch
from tensordict import TensorDict

try:
    from transfer_queue import KVBatchMeta
except ImportError:
    KVBatchMeta = None

from roll.distributed.scheduler.protocol import DataProto

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("VERL_LOGGING_LEVEL", "WARN"))

# 默认数据量阈值（字节），超过此阈值自动使用 TQ
DEFAULT_SIZE_THRESHOLD = 100 * 1024 * 1024  # 100MB

# TQ 初始化状态
_TQ_INITIALIZED = False
_TQ_INIT_LOCK = threading.Lock()


def _ensure_tq_initialized():
    """延迟初始化 TransferQueue，只在首次需要时调用。"""
    global _TQ_INITIALIZED
    if _TQ_INITIALIZED:
        return
    with _TQ_INIT_LOCK:
        if not _TQ_INITIALIZED:
            import transfer_queue as tq
            tq.init()
            _TQ_INITIALIZED = True


@dataclass
class LazyDataProto(DataProto):
    """DataProto 的惰性求值子类，数据实际存储在 TransferQueue 中。

    对用户完全透明：
    - 访问 batch / non_tensor_batch 时自动从 TQ 拉取（materialize）
    - chunk / concat / select / reorder 等操作直接在元数据层面完成，不触发数据拉取
    - 只有真正需要 tensor 数据时才触发 IO

    Attributes:
        _kv_meta: TQ 中的 KV 元数据指针
        _materialized: 是否已从 TQ 拉取数据到内存
        _tq_fields: TQ 中可用的字段列表
        _dirty_fields: 被修改过的字段集合
        _materialize_lock: 物化操作的线程锁
    """

    # ---- TQ 相关的内部状态（不参与 dataclass 的 __init__）----
    _kv_meta: Optional[KVBatchMeta] = field(default=None, repr=False, init=False)
    _materialized: bool = field(default=True, repr=False, init=False)
    _tq_fields: list = field(default_factory=list, repr=False, init=False)
    _dirty_fields: set = field(default_factory=set, repr=False, init=False)
    _materialize_lock: threading.Lock = field(default_factory=threading.Lock, repr=False, init=False)

    # ================================================================
    # 构造方法
    # ================================================================

    @classmethod
    def from_kv_batch_meta(
        cls,
        kv_meta: KVBatchMeta,
        fields: Optional[list[str]] = None,
        eager_fields: Optional[list[str]] = None,
    ) -> "LazyDataProto":
        """从 KVBatchMeta 创建 LazyDataProto，数据尚未物化。

        Args:
            kv_meta: TQ 中的 KV 元数据
            fields: TQ 中可用的所有字段名列表
            eager_fields: 需要立即拉取的字段（如 meta 信息等小数据）

        Returns:
            LazyDataProto 实例，数据尚未物化
        """
        # 使用 DataProto 的 __init__ 创建空实例
        instance = cls(batch=None, non_tensor_batch={}, meta_info={})

        # 设置 TQ 内部状态
        instance._kv_meta = kv_meta
        instance._materialized = False
        instance._tq_fields = list(fields) if fields else []
        instance._dirty_fields = set()
        instance._materialize_lock = threading.Lock()

        # 从 kv_meta 的 extra_info 中提取 meta_info
        if kv_meta.extra_info:
            instance.meta_info = dict(kv_meta.extra_info)

        # 如果有需要立即拉取的小字段
        if eager_fields:
            instance._materialize_fields(eager_fields)

        return instance

    @classmethod
    def from_data_proto(
        cls,
        data_proto: "DataProto",
        tq_partition_id: Optional[str] = None,
        size_threshold: int = DEFAULT_SIZE_THRESHOLD,
    ) -> "DataProto":
        """从普通 DataProto 创建，自动判断是否需要 TQ。

        如果 tq_partition_id 为 None 或数据量小于阈值，直接返回原始 DataProto；
        否则将数据推送到 TQ，返回 LazyDataProto。

        Args:
            data_proto: 原始 DataProto
            tq_partition_id: TQ 分区 ID，如果为 None 则不使用 TQ
            size_threshold: 数据量阈值（字节），超过则使用 TQ

        Returns:
            如果不需要 TQ，返回原始 DataProto；否则返回 LazyDataProto
        """
        if tq_partition_id is None:
            return data_proto

        data_size = cls._estimate_data_size(data_proto)
        if data_size < size_threshold:
            return data_proto

        # 将数据推送到 TQ，返回 LazyDataProto
        _ensure_tq_initialized()
        kv_meta = cls._push_data_to_tq(data_proto, tq_partition_id)
        tq_fields = list(data_proto.batch.keys()) if data_proto.batch is not None else []

        instance = cls.from_kv_batch_meta(kv_meta, fields=tq_fields)
        instance.meta_info = data_proto.meta_info
        return instance

    # ================================================================
    # 属性拦截（惰性求值的核心）
    # ================================================================

    def __getattribute__(self, name):
        """拦截 batch 和 non_tensor_batch 的访问，自动触发物化。

        对于 dataclass 字段，无法直接使用 property，因此通过
        __getattribute__ 拦截来实现惰性求值。
        """
        # 只拦截 batch 和 non_tensor_batch 的访问
        if name in ("batch", "non_tensor_batch"):
            # 避免在初始化阶段或内部方法中触发递归
            try:
                kv_meta = object.__getattribute__(self, "_kv_meta")
                materialized = object.__getattribute__(self, "_materialized")
            except AttributeError:
                # 初始化阶段，_kv_meta 还不存在
                return object.__getattribute__(self, name)

            if kv_meta is not None and not materialized:
                self._materialize()

        return object.__getattribute__(self, name)

    def __setattr__(self, name, value):
        """拦截 batch 字段的设置，追踪脏数据。"""
        # 追踪 batch 的修改
        if name == "batch" and value is not None:
            try:
                kv_meta = object.__getattribute__(self, "_kv_meta")
                if kv_meta is not None:
                    object.__setattr__(self, "_materialized", True)
            except AttributeError:
                pass

        object.__setattr__(self, name, value)

    # ================================================================
    # 核心内部方法
    # ================================================================

    def _materialize(self, fields: Optional[list[str]] = None):
        """从 TQ 拉取数据到内存（全量物化）。

        Args:
            fields: 要拉取的字段列表，None 表示拉取所有字段
        """
        lock = object.__getattribute__(self, "_materialize_lock")
        with lock:
            # 双重检查
            if object.__getattribute__(self, "_materialized"):
                return

            kv_meta = object.__getattribute__(self, "_kv_meta")
            tq_fields = object.__getattribute__(self, "_tq_fields")
            target_fields = fields or tq_fields

            if not target_fields or not kv_meta.keys:
                object.__setattr__(self, "_materialized", True)
                return

            _ensure_tq_initialized()
            import transfer_queue as tq

            logger.info(
                f"Materializing LazyDataProto: {len(kv_meta.keys)} samples, "
                f"{len(target_fields)} fields from partition '{kv_meta.partition_id}'"
            )

            data = tq.kv_batch_get(
                keys=kv_meta.keys,
                partition_id=kv_meta.partition_id,
                fields=target_fields,
            )

            # TQ 返回的是 TensorDict（可能含 nested tensor），转换为 padded tensor
            padded = data.to_padded_tensor()

            # 分离 tensor 和 non-tensor 字段
            tensor_fields = {}
            non_tensor_fields = {}
            for key in padded.keys():
                val = padded[key]
                if isinstance(val, torch.Tensor):
                    tensor_fields[key] = val
                else:
                    non_tensor_fields[key] = np.array(val.tolist(), dtype=object)

            if tensor_fields:
                batch_size = len(kv_meta.keys)
                object.__setattr__(
                    self, "batch",
                    TensorDict(tensor_fields, batch_size=(batch_size,))
                )

            current_non_tensor = object.__getattribute__(self, "non_tensor_batch")
            current_non_tensor.update(non_tensor_fields)

            object.__setattr__(self, "_materialized", True)

    def _materialize_fields(self, fields: list[str]):
        """只拉取指定字段（部分物化），不改变 _materialized 状态。

        用于拉取少量元数据字段（如 uid, tags 等），避免全量物化。

        Args:
            fields: 要拉取的字段列表
        """
        kv_meta = object.__getattribute__(self, "_kv_meta")
        if not kv_meta or not kv_meta.keys or not fields:
            return

        _ensure_tq_initialized()
        import transfer_queue as tq

        data = tq.kv_batch_get(
            keys=kv_meta.keys,
            partition_id=kv_meta.partition_id,
            fields=fields,
        )
        padded = data.to_padded_tensor()

        batch = object.__getattribute__(self, "batch")
        non_tensor_batch = object.__getattribute__(self, "non_tensor_batch")

        for key in fields:
            if key in padded.keys():
                val = padded[key]
                if isinstance(val, torch.Tensor):
                    if batch is None:
                        batch = TensorDict({key: val}, batch_size=(len(kv_meta.keys),))
                        object.__setattr__(self, "batch", batch)
                    else:
                        batch[key] = val
                else:
                    non_tensor_batch[key] = np.array(val.tolist(), dtype=object)

    def dematerialize(self):
        """将内存中修改过的数据写回 TQ，释放内存。

        只写回 _dirty_fields 中记录的字段，减少 IO。
        写回后释放 batch 和 non_tensor_batch，恢复惰性状态。
        """
        kv_meta = object.__getattribute__(self, "_kv_meta")
        dirty_fields = object.__getattribute__(self, "_dirty_fields")

        if not dirty_fields or kv_meta is None:
            return

        _ensure_tq_initialized()
        import transfer_queue as tq

        batch = object.__getattribute__(self, "batch")

        # 只写回被修改过的字段
        output = {}
        if batch is not None:
            for field_name in dirty_fields:
                if field_name in batch.keys():
                    output[field_name] = batch[field_name]

        if output:
            output_td = TensorDict(output, batch_size=(len(kv_meta.keys),))
            tq.kv_batch_put(
                keys=kv_meta.keys,
                partition_id=kv_meta.partition_id,
                fields=output_td,
            )

        # 释放内存，恢复惰性状态
        object.__setattr__(self, "batch", None)
        object.__setattr__(self, "non_tensor_batch", {})
        object.__setattr__(self, "_materialized", False)
        object.__setattr__(self, "_dirty_fields", set())

    # ================================================================
    # 重写 DataProto 的关键操作（元数据层面完成）
    # ================================================================

    def __len__(self):
        """不需要物化就能知道长度。"""
        kv_meta = object.__getattribute__(self, "_kv_meta")
        if kv_meta is not None:
            return len(kv_meta.keys)
        return super().__len__()

    def chunk(self, chunks: int) -> list["LazyDataProto"]:
        """在元数据层面分片，不触发数据拉取。

        如果数据已物化，则走原始 DataProto 的 chunk 逻辑。

        Args:
            chunks: 分片数量

        Returns:
            分片后的 LazyDataProto 列表
        """
        kv_meta = object.__getattribute__(self, "_kv_meta")
        materialized = object.__getattribute__(self, "_materialized")

        if kv_meta is not None and not materialized:
            keys = kv_meta.keys
            tags = kv_meta.tags
            total = len(keys)

            if total % chunks != 0:
                raise ValueError(
                    f"only support equal chunk. Got size of LazyDataProto {total} and chunk {chunks}."
                )

            chunk_size = total // chunks
            tq_fields = object.__getattribute__(self, "_tq_fields")
            meta_info = object.__getattribute__(self, "meta_info")

            result = []
            for i in range(chunks):
                start = i * chunk_size
                end = start + chunk_size

                sub_meta = KVBatchMeta(
                    partition_id=kv_meta.partition_id,
                    keys=keys[start:end],
                    tags=tags[start:end] if tags else [],
                    fields=kv_meta.fields,
                    extra_info=kv_meta.extra_info,
                )
                sub = LazyDataProto.from_kv_batch_meta(sub_meta, fields=tq_fields)
                sub.meta_info = meta_info
                result.append(sub)
            return result

        # 如果已经物化，走原始逻辑
        return super().chunk(chunks)

    @staticmethod
    def concat(data: list) -> "DataProto":
        """合并多个 DataProto/LazyDataProto。

        如果所有元素都是未物化的 LazyDataProto 且在同一个 partition，
        则在元数据层面合并；否则物化后走原始逻辑。

        Args:
            data: DataProto 或 LazyDataProto 的列表

        Returns:
            合并后的 LazyDataProto 或 DataProto
        """
        if not data:
            raise ValueError("Cannot concatenate an empty list.")

        # 检查是否所有元素都是未物化的 LazyDataProto
        all_lazy = all(
            isinstance(d, LazyDataProto)
            and object.__getattribute__(d, "_kv_meta") is not None
            and not object.__getattribute__(d, "_materialized")
            for d in data
        )

        if all_lazy:
            # 检查是否在同一个 partition
            partition_ids = set(
                object.__getattribute__(d, "_kv_meta").partition_id for d in data
            )
            if len(partition_ids) == 1:
                # 在元数据层面合并
                all_keys = []
                all_tags = []
                partition_id = partition_ids.pop()

                for d in data:
                    kv_meta = object.__getattribute__(d, "_kv_meta")
                    all_keys.extend(kv_meta.keys)
                    if kv_meta.tags:
                        all_tags.extend(kv_meta.tags)

                merged_meta = KVBatchMeta(
                    partition_id=partition_id,
                    keys=all_keys,
                    tags=all_tags if all_tags else [],
                    fields=object.__getattribute__(data[0], "_kv_meta").fields,
                    extra_info=object.__getattribute__(data[0], "_kv_meta").extra_info,
                )

                tq_fields = object.__getattribute__(data[0], "_tq_fields")
                result = LazyDataProto.from_kv_batch_meta(merged_meta, fields=tq_fields)

                # 合并 meta_info
                merged_meta_info = {}
                for d in data:
                    meta_info = object.__getattribute__(d, "meta_info")
                    for k, v in meta_info.items():
                        if k not in merged_meta_info:
                            merged_meta_info[k] = v
                result.meta_info = merged_meta_info
                return result

        # 否则走原始 DataProto 的 concat 逻辑
        return DataProto.concat(data)

    def select(self, batch_keys=None, non_tensor_batch_keys=None, meta_info_keys=None, deepcopy=False) -> "DataProto":
        """选择字段子集。

        如果数据未物化且只选择 batch_keys，则在元数据层面操作，不触发物化。

        Args:
            batch_keys: 要选择的 batch 字段列表
            non_tensor_batch_keys: 要选择的 non_tensor_batch 字段列表
            meta_info_keys: 要选择的 meta_info 字段列表
            deepcopy: 是否深拷贝

        Returns:
            选择后的 LazyDataProto 或 DataProto
        """
        kv_meta = object.__getattribute__(self, "_kv_meta")
        materialized = object.__getattribute__(self, "_materialized")
        tq_fields = object.__getattribute__(self, "_tq_fields")

        if (
            kv_meta is not None
            and not materialized
            and batch_keys is not None
            and non_tensor_batch_keys is None
        ):
            # 只更新 _tq_fields，不拉取数据
            selected_fields = [k for k in batch_keys if k in tq_fields]
            new = LazyDataProto.from_kv_batch_meta(kv_meta, fields=selected_fields)

            meta_info = object.__getattribute__(self, "meta_info")
            if meta_info_keys is not None:
                new.meta_info = {k: v for k, v in meta_info.items() if k in meta_info_keys}
            else:
                new.meta_info = meta_info if not deepcopy else copy.deepcopy(meta_info)
            return new

        # 否则物化后走原始逻辑
        return super().select(
            batch_keys=batch_keys,
            non_tensor_batch_keys=non_tensor_batch_keys,
            meta_info_keys=meta_info_keys,
            deepcopy=deepcopy,
        )

    def reorder(self, indices):
        """重排序，在元数据层面完成。

        如果数据未物化，直接在 KVBatchMeta 层面重排序；
        否则走原始 DataProto 的 reorder 逻辑。

        Args:
            indices: 重排序索引（torch.Tensor, list, 或 np.ndarray）
        """
        kv_meta = object.__getattribute__(self, "_kv_meta")
        materialized = object.__getattribute__(self, "_materialized")

        if kv_meta is not None and not materialized:
            if isinstance(indices, torch.Tensor):
                indices_list = indices.tolist()
            elif isinstance(indices, np.ndarray):
                indices_list = indices.tolist()
            else:
                indices_list = list(indices)

            new_keys = [kv_meta.keys[i] for i in indices_list]
            new_tags = [kv_meta.tags[i] for i in indices_list] if kv_meta.tags else []

            new_meta = KVBatchMeta(
                partition_id=kv_meta.partition_id,
                keys=new_keys,
                tags=new_tags,
                fields=kv_meta.fields,
                extra_info=kv_meta.extra_info,
            )
            object.__setattr__(self, "_kv_meta", new_meta)
            return

        super().reorder(indices)

    # ================================================================
    # 与 single-controller / TQ 的兼容接口
    # ================================================================

    def to_kv_batch_meta(self) -> KVBatchMeta:
        """导出为 KVBatchMeta，供需要直接操作 TQ 的场景使用。

        如果有脏数据，先写回 TQ。

        Returns:
            KVBatchMeta 实例

        Raises:
            ValueError: 如果不是从 TQ 创建的 LazyDataProto
        """
        kv_meta = object.__getattribute__(self, "_kv_meta")
        if kv_meta is None:
            raise ValueError("This LazyDataProto was not created from TQ")

        dirty_fields = object.__getattribute__(self, "_dirty_fields")
        if dirty_fields:
            self.dematerialize()

        return kv_meta

    @property
    def is_lazy(self) -> bool:
        """是否处于惰性模式（数据在 TQ 中，尚未物化）。"""
        kv_meta = object.__getattribute__(self, "_kv_meta")
        materialized = object.__getattribute__(self, "_materialized")
        return kv_meta is not None and not materialized

    @property
    def kv_meta(self) -> Optional[KVBatchMeta]:
        """只读访问内部的 KVBatchMeta。"""
        return object.__getattribute__(self, "_kv_meta")

    # ================================================================
    # 序列化支持
    # ================================================================

    def __getstate__(self):
        """序列化时，如果数据未物化，只序列化元数据。"""
        kv_meta = object.__getattribute__(self, "_kv_meta")
        materialized = object.__getattribute__(self, "_materialized")

        if kv_meta is not None and not materialized:
            # 只序列化元数据
            return {
                "_kv_meta": kv_meta,
                "_tq_fields": object.__getattribute__(self, "_tq_fields"),
                "meta_info": object.__getattribute__(self, "meta_info"),
                "_is_lazy_serialized": True,
            }

        # 已物化，走原始序列化逻辑
        state = super().__getstate__()
        return {
            "_original_state": state,
            "_kv_meta": kv_meta,
            "_tq_fields": object.__getattribute__(self, "_tq_fields"),
            "_is_lazy_serialized": False,
        }

    def __setstate__(self, data):
        """反序列化。"""
        if data.get("_is_lazy_serialized", False):
            # 从元数据恢复
            object.__setattr__(self, "_kv_meta", data["_kv_meta"])
            object.__setattr__(self, "_tq_fields", data["_tq_fields"])
            object.__setattr__(self, "meta_info", data["meta_info"])
            object.__setattr__(self, "batch", None)
            object.__setattr__(self, "non_tensor_batch", {})
            object.__setattr__(self, "_materialized", False)
            object.__setattr__(self, "_dirty_fields", set())
            object.__setattr__(self, "_materialize_lock", threading.Lock())
        else:
            # 从完整数据恢复
            super().__setstate__(data["_original_state"])
            object.__setattr__(self, "_kv_meta", data["_kv_meta"])
            object.__setattr__(self, "_tq_fields", data["_tq_fields"])
            object.__setattr__(self, "_materialized", True)
            object.__setattr__(self, "_dirty_fields", set())
            object.__setattr__(self, "_materialize_lock", threading.Lock())

    # ================================================================
    # 辅助方法
    # ================================================================

    @staticmethod
    def _estimate_data_size(data_proto: "DataProto") -> int:
        """估算 DataProto 的数据大小（字节）。

        Args:
            data_proto: 要估算的 DataProto

        Returns:
            估算的字节数
        """
        size = 0
        if data_proto.batch is not None:
            for _, tensor in data_proto.batch.items():
                if isinstance(tensor, torch.Tensor):
                    size += tensor.element_size() * tensor.numel()
        for _, arr in data_proto.non_tensor_batch.items():
            if isinstance(arr, np.ndarray):
                size += arr.nbytes
        return size

    @staticmethod
    def _push_data_to_tq(data_proto: "DataProto", partition_id: str) -> KVBatchMeta:
        """将 DataProto 的数据推送到 TQ。

        为每个样本生成唯一 key，将 batch 数据写入 TQ。

        Args:
            data_proto: 要推送的 DataProto
            partition_id: TQ 分区 ID

        Returns:
            KVBatchMeta 实例
        """
        import transfer_queue as tq

        n = len(data_proto)
        keys = [str(uuid.uuid4()) for _ in range(n)]
        tags = [{}] * n

        if data_proto.batch is not None:
            tq.kv_batch_put(
                keys=keys,
                partition_id=partition_id,
                fields=data_proto.batch,
            )

        return KVBatchMeta(
            partition_id=partition_id,
            keys=keys,
            tags=tags,
        )

    def mark_dirty(self, *field_names: str):
        """手动标记字段为脏数据，下次 dematerialize 时会写回 TQ。

        Args:
            field_names: 要标记的字段名
        """
        dirty = object.__getattribute__(self, "_dirty_fields")
        for name in field_names:
            dirty.add(name)

    def __repr__(self):
        kv_meta = object.__getattribute__(self, "_kv_meta")
        materialized = object.__getattribute__(self, "_materialized")

        if kv_meta is not None and not materialized:
            return (
                f"LazyDataProto(lazy=True, size={len(kv_meta.keys)}, "
                f"partition='{kv_meta.partition_id}', "
                f"fields={object.__getattribute__(self, '_tq_fields')})"
            )
        return (
            f"LazyDataProto(lazy=False, "
            f"batch={'None' if self.batch is None else dict(self.batch.keys())}, "
            f"non_tensor_batch={list(self.non_tensor_batch.keys())}, "
            f"meta_info={list(self.meta_info.keys())})"
        )

    def check_consistency(self):
        """重写一致性检查，惰性模式下跳过。"""
        kv_meta = object.__getattribute__(self, "_kv_meta")
        materialized = object.__getattribute__(self, "_materialized")

        if kv_meta is not None and not materialized:
            # 惰性模式下不需要检查
            return

        super().check_consistency()
