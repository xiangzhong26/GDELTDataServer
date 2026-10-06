#!/usr/bin/env python3
"""Install an isolated, TLS-verified GDELT tunnel on Ubuntu; standard library only."""
import argparse
import base64
import getpass
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import platform
import secrets
import shutil
import socket
import subprocess
import tarfile
import tempfile
import time

VERSION = '0.71.0'
CHECKSUMS = {
    'amd64':'84f27e39f11169f7adcef8e8b70c9329de17747b1f14dad9fb95eef5682ea716',
    'arm64':'f33c293c275d8fc68c654b6fba8f10b2551d6463d09a9fc9cffb7227eae82266',
}
MARKER = '# Managed by DSI deploy/setup-frp.py'
CONF = Path('/etc/dsi-frp')
BIN = Path('/opt/dsi-frp')


def run(args):
    subprocess.run([str(x) for x in args],check=True)


def private_write(path,text,mode=0o600):
    path = Path(path)
    with tempfile.NamedTemporaryFile(dir=path.parent,delete=False) as f:
        tmp=Path(f.name)
        f.write(text.encode()); f.flush(); os.fsync(f.fileno())
    try:
        tmp.chmod(mode); os.replace(tmp,path)
    finally:
        tmp.unlink(missing_ok=True)


def validate_pair(data):
    if not isinstance(data,dict) or data.get('schema') != 1:
        raise ValueError('配对码格式无效')
    ipaddress.IPv4Address(data['host'])
    for k in ('server_port','remote_port'):
        if type(data[k]) is not int or not 1024 <= data[k] <= 65535:
            raise ValueError('配对端口不合法')
    if data['server_port'] == data['remote_port']:
        raise ValueError('两个端口不能相同')
    token=data['token']
    if not isinstance(token,str) or len(token)!=64 or any(c not in '0123456789abcdef' for c in token):
        raise ValueError('配对令牌格式无效')
    cert=data['certificate']
    if not isinstance(cert,str) or len(cert)>16384 or not cert.startswith('-----BEGIN CERTIFICATE-----\n') or not cert.rstrip().endswith('-----END CERTIFICATE-----'):
        raise ValueError('配对证书格式无效')
    return data


def encode_pair(data):
    validate_pair(data)
    return base64.urlsafe_b64encode(json.dumps(data,separators=(',',':')).encode()).decode()


def decode_pair(text):
    if len(text)>32768:
        raise ValueError('配对码过长')
    try:
        return validate_pair(json.loads(base64.b64decode(text.strip(),altchars=b'-_',validate=True)))
    except (ValueError,KeyError,TypeError) as exc:
        raise ValueError('配对码无效，请复制云端 pairing.txt 中的完整单行内容') from exc


def port_available(port,host='0.0.0.0'):
    try:
        with socket.socket() as sock:sock.bind((host,port))
        return True
    except OSError:
        return False


def choose_server_port(requested=None):
    if requested is not None:
        if type(requested) is not int or not 1024 <= requested <= 65535 or requested==18810:
            raise ValueError('连接端口须为1024至65535，且不能是结果端口18810')
        if not port_available(requested):
            raise ValueError('指定连接端口 '+str(requested)+' 已被占用，请换一个端口，现有服务未被修改')
        return requested
    for port in range(7000,7011):
        if port_available(port):return port
    raise ValueError('7000至7010均不可用，请用 --server-port 指定其他空闲端口')


def render_config(role,data,local_port=8800,folder=CONF):
    validate_pair(data)
    q=lambda s:json.dumps(s.as_posix() if isinstance(s,Path) else str(s))
    auth='auth.method = "token"\nauth.token = '+q(data['token'])+'\nauth.additionalScopes = ["HeartBeats", "NewWorkConns"]\n'
    if role=='cloud':
        return MARKER+'\n'+f'''bindAddr = "0.0.0.0"
bindPort = {data['server_port']}
proxyBindAddr = "127.0.0.1"
allowPorts = [{{ single = {data['remote_port']} }}]
maxPortsPerClient = 1
transport.tls.force = true
transport.tls.certFile = {q(folder/'server.crt')}
transport.tls.keyFile = {q(folder/'server.key')}
'''+auth
    if role!='local' or type(local_port) is not int or not 1 <= local_port <= 65535:
        raise ValueError('本地端口无效')
    return MARKER+'\n'+f'''serverAddr = {q(data['host'])}
serverPort = {data['server_port']}
loginFailExit = false
transport.tls.enable = true
transport.tls.trustedCaFile = {q(folder/'server.crt')}
transport.tls.serverName = {q(data['host'])}
'''+auth+f'''
[[proxies]]
name = "gdelt-results"
type = "tcp"
localIP = "127.0.0.1"
localPort = {local_port}
remotePort = {data['remote_port']}
'''


def render_service(binary,config):
    return MARKER+f'''
[Unit]
Description=DSI GDELT tunnel ({binary})
After=network-online.target
Wants=network-online.target

[Service]
User=dsi-frp
Group=dsi-frp
ExecStart={(BIN/binary).as_posix()} -c {config.as_posix()}
Restart=always
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
UMask=0077

[Install]
WantedBy=multi-user.target
'''


def download_archive(url,path):
    print('连接 GitHub：连接超时15秒；下载不限总时长，失败最多重试2次。下方显示进度，可按 Ctrl+C 取消。',flush=True)
    try:
        subprocess.run(['curl','--fail','--location','--progress-bar',
            '--connect-timeout','15','--max-filesize',str(100*1024**2),
            '--retry','2','--retry-delay','2','--retry-all-errors',
            '--output',str(path),url],check=True)
    except (subprocess.CalledProcessError,subprocess.TimeoutExpired,OSError) as exc:
        raise RuntimeError('GitHub 下载失败。可在其他电脑下载官方安装包，再用 --archive 指定文件；现有隧道未修改') from exc


def install_binary(binary,archive_source=None):
    machine=platform.machine()
    arch={'x86_64':'amd64','aarch64':'arm64','arm64':'arm64'}.get(machine)
    if not arch:
        raise ValueError('仅支持 Ubuntu amd64/arm64')
    target=BIN/binary; receipt=BIN/(binary+'.sha256'); version_file=BIN/(binary+'.version')
    if target.exists() and receipt.exists() and version_file.exists() and version_file.read_text().strip()==VERSION and hashlib.sha256(target.read_bytes()).hexdigest()==receipt.read_text().strip():
        return
    name=f'frp_{VERSION}_linux_{arch}'
    url=f'https://github.com/fatedier/frp/releases/download/v{VERSION}/{name}.tar.gz'
    print(('校验本地安装包 ' if archive_source else '从 frp 官方 GitHub 下载并校验 ')+name+'（已有其他 frp 不会被替换）',flush=True)
    with tempfile.TemporaryDirectory(prefix='dsi-frp-') as work:
        archive=Path(archive_source).expanduser().resolve() if archive_source else Path(work)/'frp.tar.gz'
        if archive_source:
            if not archive.is_file() or archive.stat().st_size>100*1024**2:
                raise ValueError('本地安装包不存在或超出100MB，请检查 --archive 文件路径')
        else:
            download_archive(url,archive)
        print('下载/读取完成，正在校验 SHA256…',flush=True)
        if hashlib.sha256(archive.read_bytes()).hexdigest()!=CHECKSUMS[arch]:
            raise ValueError('frp 官方安装包 SHA256 校验失败，停止安装')
        with tarfile.open(archive,'r:gz') as tar:
            member=tar.getmember(name+'/'+binary)
            if not member.isfile() or member.size>100*1024**2:raise ValueError('安装包内容无效')
            with tar.extractfile(member) as source:
                content=source.read()
        BIN.mkdir(mode=0o755,parents=True,exist_ok=True)
        tmp=BIN/(binary+'.new');tmp.write_bytes(content);tmp.chmod(0o755);os.replace(tmp,target)
        private_write(receipt,hashlib.sha256(content).hexdigest()+'\n')
        private_write(version_file,VERSION+'\n')


def main():
    parser=argparse.ArgumentParser(description='DSI 专用 frp 配置：不修改或重启 GDELT/DSI 服务')
    parser.add_argument('role',choices=['cloud','local'])
    parser.add_argument('--public-ip',help='cloud 模式的腾讯云公网 IPv4 地址')
    parser.add_argument('--local-port',type=int,default=8800)
    parser.add_argument('--server-port',type=int,help='cloud 连接端口；新安装默认从7000至7010选择空闲端口')
    parser.add_argument('--archive',type=Path,help='使用已下载的官方Linux安装包，仍验证固定SHA256，跳过GitHub下载')
    args=parser.parse_args()
    if platform.system()!='Linux' or os.geteuid()!=0:
        parser.error('请在 Ubuntu 使用 sudo python3 deploy/setup-frp.py ...')
    if args.role=='cloud' and not args.public_ip:parser.error('cloud 需要 --public-ip')
    if args.public_ip:ipaddress.IPv4Address(args.public_ip)
    if not 1<=args.local_port<=65535:parser.error('本地端口须为1至65535')
    if args.server_port is not None and (args.role!='cloud' or not 1024<=args.server_port<=65535 or args.server_port==18810):
        parser.error('--server-port 仅用于cloud，范围1024至65535且不能为18810')
    binary='frps' if args.role=='cloud' else 'frpc'
    service='dsi-'+binary+'.service';unit=Path('/etc/systemd/system')/service
    config=CONF/(binary+'.toml');state=CONF/(binary+'-pair.json')
    for p in (config,unit):
        if p.exists() and not p.read_text().startswith(MARKER):
            raise ValueError('发现非本脚本管理的配置，保留原文件，请先处理：'+str(p))
    data=None
    if state.exists():
        data=validate_pair(json.loads(state.read_text()))
        if args.role=='cloud' and data['host']!=args.public_ip:
            raise ValueError('现有配对地址不同，已保留配置，请使用原地址或先迁移证书')
        if args.role=='cloud' and args.server_port is not None and data['server_port']!=args.server_port:
            raise ValueError('已有配对使用端口 '+str(data['server_port'])+'，已保留配置；不能单独更换云端端口')
        print('沿用已有配对，不更换令牌或证书')
    elif args.role=='local':
        data=decode_pair(getpass.getpass('粘贴云端 pairing.txt 的配对码（不回显），然后回车：'))
    server_port=None
    if args.role=='cloud':
        active=subprocess.run(['systemctl','is-active','--quiet',service]).returncode==0
        server_port=data['server_port'] if data else choose_server_port(args.server_port)
        if not active:
            if data and not port_available(server_port):
                raise ValueError('已有配对的连接端口 '+str(server_port)+' 被其他程序占用，现有配对未改变')
            if not port_available(18810,'127.0.0.1'):
                raise ValueError('结果端口18810已被占用，请确认已有 frp/SSH 隧道，现有服务未被修改')
        print('frp 连接端口：'+str(server_port)+'；云端内部结果端口：18810',flush=True)
    if shutil.which('openssl') is None or shutil.which('curl') is None:
        run(['apt-get','update']);run(['apt-get','install','-y','openssl','curl','ca-certificates'])
    install_binary(binary,args.archive)
    CONF.mkdir(mode=0o750,parents=True,exist_ok=True)
    if data is None:
        cert=CONF/'server.crt';key=CONF/'server.key'
        run(['openssl','req','-x509','-newkey','rsa:3072','-sha256','-nodes','-days','3650',
             '-keyout',key,'-out',cert,'-subj','/CN=DSI GDELT tunnel',
             '-addext','subjectAltName=IP:'+args.public_ip,
             '-addext','basicConstraints=critical,CA:TRUE'])
        key.chmod(0o600)
        data={'schema':1,'host':args.public_ip,'server_port':server_port,'remote_port':18810,
              'token':secrets.token_hex(32),'certificate':cert.read_text()}
    validate_pair(data)
    private_write(state,json.dumps(data))
    if args.role=='cloud':
        private_write(CONF/'pairing.txt',encode_pair(data)+'\n')
    else:
        private_write(CONF/'server.crt',data['certificate'])
    rendered=render_config(args.role,data,args.local_port)
    candidate=CONF/(binary+'.candidate.toml');private_write(candidate,rendered)
    try:
        run([BIN/binary,'verify','-c',candidate])
        os.replace(candidate,config)
    finally:
        candidate.unlink(missing_ok=True)
    if subprocess.run(['id','dsi-frp'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode:
        run(['useradd','--system','--no-create-home','--shell','/usr/sbin/nologin','dsi-frp'])
    shutil.chown(CONF,user='root',group='dsi-frp');CONF.chmod(0o750)
    for p in (config,CONF/'server.crt',CONF/'server.key'):
        if p.exists():shutil.chown(p,user='root',group='dsi-frp');p.chmod(0o640)
    private_write(unit,render_service(binary,config),0o644)
    run(['systemctl','daemon-reload']);run(['systemctl','enable',service]);run(['systemctl','restart',service])
    time.sleep(1);run(['systemctl','is-active',service])
    print('已配置 '+service+'：开机启动、断线重连。GDELT 回填未被停止。')
    if args.role=='cloud':
        port=str(data['server_port'])
        print('腾讯云安全组允许 TCP '+port+' 入站；若启用 UFW，也需允许'+port+'。无需开放18810。')
        print('执行 sudo cat /etc/dsi-frp/pairing.txt，把单行配对码复制到本地安装脚本。不要发送到聊天或提交GitHub。')
    else:
        print('隧道已启动；连通状态请看 sudo journalctl -u dsi-frpc.service -n 30 --no-pager')
        print('DSI 后台源地址填 http://127.0.0.1:18810；填写的是 GDELT snapshot_read_token，不是 frp 配对码。')


if __name__=='__main__':
    try:
        main()
    except (ValueError,RuntimeError,OSError,subprocess.CalledProcessError) as exc:
        if isinstance(exc,(ValueError,RuntimeError)):
            print(str(exc))
        print('安装未完成；请查看上方步骤输出和系统日志。现有 GDELT/DSI 服务未被修改。')
        raise SystemExit(1)
