#!/bin/bash
set +x
ulimit -n 65536
ASCEND_RT_VISIBLE_DEVICES=8,9,10,11,12,13,14,15
CONFIG_PATH=$(basename $(dirname $0))
python examples/start_rlvr_pipeline.py --config_path $CONFIG_PATH  --config_name rlvr_zero3_sp2
