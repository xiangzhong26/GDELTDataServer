#!/usr/bin/env python3
"""Locate the published result without printing credentials or opening the database."""
import argparse
import json
from pathlib import Path

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--config', type=Path, default=root/'config.local.json')
args = parser.parse_args()
config_path = args.config.resolve()
config = json.loads(config_path.read_text(encoding='utf-8-sig'))
folder = Path(config.get('data_dir', 'data'))
if not folder.is_absolute():
    folder = config_path.parent/folder
folder = folder.resolve()/'snapshots'
pointer = folder/'latest.json'
if not pointer.is_file():
    raise SystemExit(f'尚未发布快照：{pointer}')
manifest = json.loads(pointer.read_text(encoding='utf-8'))
path = (folder/manifest['filename']).resolve()
if path.parent != folder or not path.is_file():
    raise SystemExit('快照指针文件无效')
print('当前互通数据：', path)
print('版本元信息：', pointer)
print('数据版本：', manifest['data_version'])
print('发布时间：', manifest['created_at'])
print('压缩大小：', round(manifest['bytes']/1024**2, 2), 'MiB')
print('校验值：', manifest['sha256'])
print('如需提供诊断样本，只提供该 .json.gz 文件；勿提供 config.local.json。')
