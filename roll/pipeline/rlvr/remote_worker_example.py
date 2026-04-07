"""
Example: Remote Worker Reading Data from TransferQueue

This script demonstrates how a remote worker can read data from TransferQueue
that was written by the main RLVR pipeline controller.

Usage:
    python remote_worker_example.py --partition_id train_step_0 --keys 0_0,0_1,0_2
"""

import argparse
import sys
from pathlib import Path

import ray
import torch
from tensordict import TensorDict

sys.path.insert(0, str(Path(__file__).parent.parent / "ROLL_a1ways"))

import transfer_queue as tq
from roll.distributed.scheduler.protocol import DataProto


def parse_args():
    parser = argparse.ArgumentParser(description="Remote worker reading from TransferQueue")
    parser.add_argument("--partition_id", type=str, required=True, help="Partition ID to read from")
    parser.add_argument("--keys", type=str, required=True, help="Comma-separated list of keys to retrieve")
    parser.add_argument("--select_fields", type=str, default=None, help="Comma-separated list of fields to retrieve")
    return parser.parse_args()


def read_data_from_tq(partition_id: str, keys: list[str], select_fields: list[str] = None):
    """
    Read data from TransferQueue.
    
    Args:
        partition_id: Partition ID in TransferQueue
        keys: List of keys to retrieve
        select_fields: Optional list of specific fields to retrieve
    """
    print(f"Reading data from TransferQueue:")
    print(f"  Partition ID: {partition_id}")
    print(f"  Keys: {keys}")
    print(f"  Select fields: {select_fields}")
    
    data = tq.kv_batch_get(keys=keys, partition_id=partition_id, select_fields=select_fields)
    
    print(f"\nRetrieved data:")
    print(f"  Batch size: {data.batch_size}")
    print(f"  Fields: {list(data.keys())}")
    
    return data


def reconstruct_dataproto(data: TensorDict) -> DataProto:
    """
    Reconstruct DataProto from TensorDict retrieved from TransferQueue.
    
    Args:
        data: TensorDict from TransferQueue
        
    Returns:
        DataProto object
    """
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
    data_proto = DataProto(batch=batch, non_tensor_batch=non_tensor_dict, meta_info={})
    
    print(f"\nReconstructed DataProto:")
    print(f"  Batch size: {data_proto.batch.batch_size}")
    print(f"  Tensor fields: {list(data_proto.batch.keys())}")
    print(f"  Non-tensor fields: {list(data_proto.non_tensor_batch.keys())}")
    
    return data_proto


def process_data(data_proto: DataProto):
    """
    Example processing function for the retrieved data.
    
    Args:
        data_proto: DataProto containing the batch data
    """
    print(f"\nProcessing data:")
    
    if "prompts" in data_proto.batch:
        prompts = data_proto.batch["prompts"]
        print(f"  Prompts shape: {prompts.shape}")
        print(f"  Prompts dtype: {prompts.dtype}")
    
    if "responses" in data_proto.batch:
        responses = data_proto.batch["responses"]
        print(f"  Responses shape: {responses.shape}")
        print(f"  Responses dtype: {responses.dtype}")
    
    if "domain" in data_proto.non_tensor_batch:
        domains = data_proto.non_tensor_batch["domain"]
        print(f"  Domains: {domains}")


def main():
    args = parse_args()
    
    keys = args.keys.split(",")
    select_fields = args.select_fields.split(",") if args.select_fields else None
    
    print("=" * 80)
    print("Remote Worker: Reading Data from TransferQueue")
    print("=" * 80)
    
    if not ray.is_initialized():
        ray.init(namespace="TransferQueueExample")
    
    print("\nInitializing TransferQueue client...")
    tq.init()
    print("TransferQueue client initialized")
    
    try:
        data = read_data_from_tq(args.partition_id, keys, select_fields)
        
        data_proto = reconstruct_dataproto(data)
        
        process_data(data_proto)
        
        print("\n" + "=" * 80)
        print("Remote worker completed successfully!")
        print("=" * 80)
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        tq.close()
        ray.shutdown()


if __name__ == "__main__":
    main()
