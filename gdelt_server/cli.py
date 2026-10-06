from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
from pathlib import Path
import ssl
from urllib.parse import urlparse
import httpx
import uvicorn

from .config import Settings


def main():
    parser = argparse.ArgumentParser(description="GDELT聚合数据服务器")
    parser.add_argument("--config", help="配置文件路径")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("serve", help="启动本地采集、计算与管理页面")
    sub.add_parser("receiver", help="启动只接收结果的云端服务")
    sub.add_parser("selftest", help="真实下载少量Events/GKG批次并测试解析、计算、快照，退出清理测试数据")
    benchmark = sub.add_parser('benchmark', help='隔离数据目录，对照串行与并行采集速度，不修改现有进度')
    benchmark.add_argument('--slots', type=int, default=4, choices=range(1, 13), help='对照1至12个十五分钟时段，默认4个')
    sub.add_parser('doctor', help='检查存储权限、临时目录和数据库完整性，不修复或删除数据')
    push = sub.add_parser("push", help="上传最新结果快照")
    push.add_argument("--url", required=True, help="接收器完整地址，例如 https://域名/api/snapshots")
    push.add_argument("--token-env", default="GDELT_PUSH_TOKEN", help="上传令牌所在环境变量名")
    push.add_argument("--ca-file", help="可选的自签证书CA文件；不关闭证书验证")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = Settings.load(args.config)
    if args.command == "serve":
        from .app import create_app
        uvicorn.run(create_app(settings), host=settings.host, port=settings.port, workers=1)
    elif args.command == "receiver":
        from .receiver import create_receiver
        uvicorn.run(create_receiver(settings), host=settings.host, port=settings.port, workers=1)
    elif args.command == "selftest":
        from .selftest import real_selftest
        print(json.dumps(real_selftest(settings), ensure_ascii=False, indent=2))
    elif args.command == 'doctor':
        from .diagnostics import diagnose
        result = diagnose(settings)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        if not result['ok']:
            raise SystemExit(1)
    elif args.command == 'benchmark':
        from .benchmark import real_benchmark
        print(json.dumps(real_benchmark(settings, args.slots), ensure_ascii=False, indent=2))
    else:
        parsed = urlparse(args.url)
        if parsed.scheme != "https" and not (parsed.scheme == "http" and parsed.hostname in ("localhost", "127.0.0.1", "::1")):
            parser.error("远程上传必须使用HTTPS；本机测试可用HTTP")
        token = os.environ.get(args.token_env)
        if not token:
            parser.error(f"请在环境变量 {args.token_env} 中配置接收端令牌")
        from .snapshot import SnapshotFiles
        files = SnapshotFiles(settings.data_dir/"snapshots")
        manifest = files.manifest()
        if not manifest:
            parser.error("尚未生成快照，请先在控制页面导出")
        payload = (files.folder/manifest["filename"]).read_bytes()
        if hashlib.sha256(payload).hexdigest() != manifest["sha256"]:
            parser.error("本地快照校验失败")
        verify = ssl.create_default_context(cafile=args.ca_file) if args.ca_file else True
        with httpx.Client(timeout=120, verify=verify) as client:
            response = client.put(args.url, content=payload,
                                  headers={"Authorization": "Bearer "+token, "X-SHA256": manifest["sha256"],
                                           "Content-Type": "application/gzip"})
            response.raise_for_status()
            print(json.dumps(response.json(), ensure_ascii=False))


if __name__ == "__main__":
    main()
