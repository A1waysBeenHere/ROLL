# TransferQueue Integration with RLVR Pipeline

## Overview

This document describes the integration of TransferQueue into the RLVR (Reinforcement Learning from Verifiable Rewards) pipeline. The integration follows a specific pattern where the main controller writes data to TransferQueue, and remote workers can read data from TransferQueue for subsequent processing.

## Integration Pattern

### Data Flow

```
┌─────────────────────┐
│   Main Controller   │
│  (RLVR Pipeline)    │
└──────────┬──────────┘
           │
           │ 1. Get data via Ray RPC
           ▼
┌─────────────────────┐
│   Remote Workers    │
│  (Generate/Reward)  │
└──────────┬──────────┘
           │
           │ 2. Write to TransferQueue
           ▼
┌─────────────────────┐
│   TransferQueue     │
│  (Storage Backend)  │
└──────────┬──────────┘
           │
           │ 3. Read by remote workers
           ▼
┌─────────────────────┐
│   Remote Workers    │
│  (Train/Inference)  │
└─────────────────────┘
```

## Key Changes

### 1. Imports

Added TransferQueue and TensorDict imports:

```python
import transfer_queue as tq
from tensordict import TensorDict
```

### 2. Initialization

TransferQueue is initialized in the `__init__` method:

```python
# Initialize TransferQueue
logger.info("Initializing TransferQueue...")
tq_conf = tq.init()
logger.info(f"TransferQueue initialized with config: {tq_conf}")
```

### 3. Data Write

After generating rollout data, it's written to TransferQueue:

```python
# Write generated data to TransferQueue
partition_id = f"train_step_{global_step}"
try:
    keys = self._write_to_transfer_queue(batch, partition_id, global_step)
    batch.meta_info["tq_keys"] = keys
    batch.meta_info["tq_partition_id"] = partition_id
    logger.info(f"Generated data written to TransferQueue: partition={partition_id}")
except Exception as e:
    logger.warning(f"Failed to write generated data to TransferQueue: {e}")
```

### 4. Data Read

Remote workers can read data from TransferQueue:

```python
# In a remote worker
partition_id = f"train_step_{global_step}"
keys = [f"{global_step}_{i}" for i in range(batch_size)]

# Read data
data = pipeline._read_from_transfer_queue(keys, partition_id)

# Or reconstruct as DataProto
data_proto = pipeline._reconstruct_batch_from_tq(keys, partition_id)
```

### 5. Cleanup

TransferQueue is properly closed when the pipeline completes:

```python
# Close TransferQueue
logger.info("Closing TransferQueue...")
try:
    tq.close()
    logger.info("TransferQueue closed successfully")
except Exception as e:
    logger.warning(f"Failed to close TransferQueue: {e}")
```

## Key Features

### Partition-Based Organization

Each training step has its own partition in TransferQueue:
- Partition ID format: `train_step_{global_step}`
- Enables isolation between different training steps
- Facilitates data lifecycle management

### Key-Based Access

Data is keyed using the format: `{global_step}_{sample_index}`
- Example: `0_0`, `0_1`, `0_2` for the first 3 samples at step 0
- Enables fine-grained data access
- Supports selective field retrieval

### Metadata Preservation

Both tensor and non-tensor data are stored:
- **Tensor data**: Stored directly in TensorDict
- **Non-tensor data**: Prefixed with `non_tensor_` and converted to tensors
- **Tags**: Include metadata like `global_step` and `domain`

### Error Handling

Graceful degradation if TransferQueue operations fail:
- Write failures are logged but don't stop training
- Read failures raise exceptions with clear error messages
- System continues to function even if TransferQueue is unavailable

## Usage Examples

### Example 1: Remote Worker Reading Data

```python
import transfer_queue as tq
from roll.distributed.scheduler.protocol import DataProto
from tensordict import TensorDict

# Initialize TransferQueue client
tq.init()

# Define partition and keys
partition_id = "train_step_0"
keys = ["0_0", "0_1", "0_2"]

# Read specific fields
data = tq.kv_batch_get(
    keys=keys,
    partition_id=partition_id,
    select_fields=["prompts", "responses"]
)

# Process data
print(f"Retrieved {data.batch_size[0]} samples")
print(f"Fields: {list(data.keys())}")
```

### Example 2: Reconstructing DataProto

```python
def reconstruct_dataproto(data: TensorDict) -> DataProto:
    """Reconstruct DataProto from TransferQueue data."""
    batch_size = data.batch_size[0]
    
    tensor_dict = {}
    non_tensor_dict = {}
    
    for key in data.keys():
        if key.startswith("non_tensor_"):
            non_tensor_key = key.replace("non_tensor_", "")
            non_tensor_dict[non_tensor_key] = data[key].numpy()
        else:
            tensor_dict[key] = data[key]
    
    batch = TensorDict(tensor_dict, batch_size=[batch_size])
    return DataProto(batch=batch, non_tensor_batch=non_tensor_dict, meta_info={})
```

### Example 3: Selective Field Retrieval

```python
# Only retrieve prompts and attention masks
data = tq.kv_batch_get(
    keys=["0_0", "0_1"],
    partition_id="train_step_0",
    select_fields=["prompts", "attention_mask"]
)
```

## Benefits

1. **Decoupling**: Separates data producers from consumers
2. **Asynchronous Access**: Enables non-blocking data operations
3. **Fine-Grained Visibility**: Track production/consumption status at sample level
4. **Streaming Support**: Process data in a streaming manner
5. **Scalability**: Distributed storage backend supports large-scale training

## Configuration

TransferQueue can be configured via the `tq.init()` call:

```python
from omegaconf import DictConfig

config = DictConfig({
    "backend": {
        "storage_backend": "SimpleStorage",  # or "MooncakeStore", "Yuanrong"
        "SimpleStorage": {
            "num_data_storage_units": 4,
            "total_storage_size": 1000000
        }
    }
})

tq.init(config)
```

## Performance Considerations

1. **Batch Size**: Larger batch sizes improve throughput but increase memory usage
2. **Field Selection**: Use `select_fields` to retrieve only necessary data
3. **Partition Strategy**: Balance between isolation and overhead
4. **Storage Backend**: Choose appropriate backend based on requirements:
   - **SimpleStorage**: Fast, in-memory, good for development
   - **MooncakeStore**: High-performance, RDMA support
   - **Yuanrong**: Hierarchical storage (HBM/DRAM/SSD)

## Troubleshooting

### Issue: TransferQueue initialization fails

**Solution**: Ensure Ray is properly initialized and TransferQueue package is installed:
```bash
pip install TransferQueue
```

### Issue: Data not found in TransferQueue

**Solution**: Check partition ID and keys:
```python
# List all partitions
partitions = tq.kv_list()
print(f"Available partitions: {partitions}")

# Check specific partition
partition_info = tq.kv_list(partition_id="train_step_0")
print(f"Keys in partition: {list(partition_info['train_step_0'].keys())}")
```

### Issue: Memory overflow

**Solution**: Clear old partitions periodically:
```python
# Clear a specific partition
tq.kv_clear(keys=["0_0", "0_1"], partition_id="train_step_0")
```

## References

- [TransferQueue Documentation](https://github.com/Ascend/TransferQueue)
- [TransferQueue Paper](https://arxiv.org/abs/2507.01663)
- [RLVR Pipeline Documentation](link-to-rlvr-docs)
