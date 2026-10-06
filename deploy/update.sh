#!/usr/bin/env bash
# Run as your ordinary login user so uv can be located before sudo.
set -Eeuo pipefail

main() {
SCRIPT_PATH="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)/$(basename -- "${BASH_SOURCE[0]}")"
PROJECT_DIR="$(dirname -- "$(dirname -- "$SCRIPT_PATH")")"
SERVICE_NAME=gdelt-data-server

if [[ "$(id -u)" != 0 ]]; then
  GDELT_UPDATE_UV="$(command -v uv || true)"
  if [[ -z "$GDELT_UPDATE_UV" ]]; then
    echo '找不到uv，请先在当前登录用户环境安装uv。' >&2
    exit 1
  fi
  exec sudo env "GDELT_UPDATE_UV=$GDELT_UPDATE_UV" bash "$SCRIPT_PATH" "$@"
fi

if [[ $# != 0 ]]; then
  echo '用法：bash /opt/GDELTDataServer/deploy/update.sh' >&2
  exit 1
fi
if [[ "$PROJECT_DIR" != /opt/GDELTDataServer ]]; then
  echo '此更新脚本适用于README中的 /opt/GDELTDataServer 部署方案。' >&2
  exit 1
fi
cd -- "$PROJECT_DIR"
GDELT_UPDATE_UV="${GDELT_UPDATE_UV:-$(command -v uv || true)}"
if [[ ! -x "$GDELT_UPDATE_UV" || ! -x .venv/bin/python || ! -f config.local.json ]]; then
  echo '缺少uv、项目环境或本地配置，请先完成README部署步骤。' >&2
  exit 1
fi
if [[ -n "$(git status --porcelain)" ]]; then
  echo '服务器项目存在未提交修改或未跟踪文件；为保护这些内容，更新已停止。请先查看 git status。' >&2
  exit 1
fi
UPSTREAM="$(git rev-parse --abbrev-ref --symbolic-full-name '@{upstream}')"
BRANCH="$(git symbolic-ref --short HEAD)"
REMOTE="$(git config "branch.$BRANCH.remote")"
if [[ -z "$REMOTE" || "$REMOTE" == . ]]; then
  echo '当前分支未配置GitHub远程跟踪分支。' >&2
  exit 1
fi

echo '1/6 从GitHub获取更新（服务此时仍在运行）…'
git fetch "$REMOTE"
if ! git merge-base --is-ancestor HEAD "$UPSTREAM"; then
  echo '服务器分支与远程分支分叉，无法安全快进；服务未停止，请先处理分支差异。' >&2
  exit 1
fi

UPDATE_STOPPED=0
on_error() {
  local code=$?
  trap - ERR
  if [[ "$UPDATE_STOPPED" == 1 ]]; then
    systemctl stop "$SERVICE_NAME" || true
    echo '更新未完成，服务已保持停止。数据和配置保留；请处理上面的错误后重新运行本命令。' >&2
  else
    echo '获取更新失败，现有服务未被停止。请检查网络、GitHub认证或分支配置。' >&2
  fi
  exit "$code"
}
trap on_error ERR

echo '2/6 停止服务并快进更新代码…'
systemctl stop "$SERVICE_NAME"
UPDATE_STOPPED=1
git merge --ff-only "$UPSTREAM"

echo '3/6 使用uv安装依赖…'
env UV_PYTHON_INSTALL_DIR=/opt/gdelt-python "$GDELT_UPDATE_UV" pip install \
  --python "$PROJECT_DIR/.venv/bin/python" -e '.[test]'

echo '4/6 执行自动化测试…'
.venv/bin/python -m pytest -q

echo '5/6 更新服务模板并重启…'
install -m 644 deploy/gdelt-data-server.service /etc/systemd/system/gdelt-data-server.service
systemctl daemon-reload
systemctl start "$SERVICE_NAME"

echo '6/6 检查服务和健康接口…'
.venv/bin/python - <<'PY'
import json
import time
import urllib.request
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
last_error = None
for _ in range(15):
    try:
        with opener.open('http://127.0.0.1:8800/health', timeout=2) as response:
            data = json.load(response)
        if data.get('ok') is True and data.get('service') == 'gdelt-data-server':
            break
        raise RuntimeError(f'健康检查返回异常：{data}')
    except Exception as exc:
        last_error = exc
        time.sleep(1)
else:
    raise SystemExit(f'健康检查失败：{last_error}')
PY
systemctl is-active --quiet "$SERVICE_NAME"
UPDATE_STOPPED=0
echo "更新完成，当前版本：$(git rev-parse --short HEAD)。请在管理页面按Ctrl+F5刷新。"
}

# Parse the complete function before pulling: Git may replace this script while
# it runs, but the current update continues with one coherent script version.
main "$@"
