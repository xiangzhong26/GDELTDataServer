import copy
import importlib.util
from pathlib import Path
import tomllib
import subprocess

import pytest

spec=importlib.util.spec_from_file_location('setup_frp',Path(__file__).resolve().parent.parent/'deploy/setup-frp.py')
frp=importlib.util.module_from_spec(spec);spec.loader.exec_module(frp)
PAIR={'schema':1,'host':'124.223.191.114','server_port':7000,'remote_port':18810,
      'token':'a'*64,'certificate':'-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----\n'}


def test_pair_roundtrip_and_no_secret_in_unit():
    assert frp.decode_pair(frp.encode_pair(PAIR))==PAIR
    unit=frp.render_service('frpc',Path('/etc/dsi-frp/frpc.toml'))
    assert PAIR['token'] not in unit
    assert 'User=dsi-frp' in unit and 'Restart=always' in unit
    assert 'ExecStart=/opt/dsi-frp/frpc -c /etc/dsi-frp/frpc.toml' in unit


def test_cloud_limits_port_and_requires_tls_and_auth():
    config=tomllib.loads(frp.render_config('cloud',PAIR))
    assert config['proxyBindAddr']=='127.0.0.1'
    assert config['allowPorts']==[{'single':18810}]
    assert config['transport']['tls']['force']
    assert config['auth']['token']==PAIR['token']
    assert config['auth']['additionalScopes']==['HeartBeats','NewWorkConns']


def test_local_checks_certificate_and_forwards_only_loopback():
    config=tomllib.loads(frp.render_config('local',PAIR,8801))
    assert config['transport']['tls']['trustedCaFile']=='/etc/dsi-frp/server.crt'
    assert config['transport']['tls']['serverName']==PAIR['host']
    assert config['loginFailExit'] is False
    assert config['proxies']==[{'name':'gdelt-results','type':'tcp','localIP':'127.0.0.1','localPort':8801,'remotePort':18810}]


@pytest.mark.parametrize('change',[{'host':'example.com'},{'host':'1.2.3.4;bad'},
    {'token':'secret'},{'token':'a'*63+'"'},{'server_port':True},
    {'remote_port':7000},{'remote_port':22},{'certificate':'bad'},{'schema':2}])
def test_invalid_pair_rejected(change):
    value=copy.deepcopy(PAIR);value.update(change)
    with pytest.raises(ValueError):frp.validate_pair(value)


@pytest.mark.parametrize('text',['not base64','e30=','a'*32769],ids=['bad-base64','missing-fields','too-long'])
def test_invalid_code_does_not_echo_secret(text):
    with pytest.raises(ValueError) as error:frp.decode_pair(text)
    assert text not in str(error.value)


def test_atomic_private_write_preserves_pair_on_repeated_install(tmp_path):
    target=tmp_path/'pairing.txt'
    frp.private_write(target,frp.encode_pair(PAIR))
    frp.private_write(target,frp.encode_pair(PAIR))
    assert frp.decode_pair(target.read_text())==PAIR
    assert list(tmp_path.iterdir())==[target]


def test_busy_7000_selects_next_free_port(monkeypatch):
    monkeypatch.setattr(frp,'port_available',lambda port:port>=7002)
    assert frp.choose_server_port()==7002
    assert frp.choose_server_port(7100)==7100
    with pytest.raises(ValueError):frp.choose_server_port(7000)


def test_no_port_available_leaves_services_untouched(monkeypatch):
    monkeypatch.setattr(frp,'port_available',lambda port:False)
    with pytest.raises(ValueError,match='--server-port'):frp.choose_server_port()


@pytest.mark.parametrize('port',[22,18810,65536,True])
def test_invalid_control_port(port):
    with pytest.raises(ValueError):frp.choose_server_port(port)


def test_custom_control_port_is_transferred_in_pairing():
    data={**PAIR,'server_port':7001}
    decoded=frp.decode_pair(frp.encode_pair(data))
    assert tomllib.loads(frp.render_config('cloud',decoded))['bindPort']==7001
    assert tomllib.loads(frp.render_config('local',decoded))['serverPort']==7001


def test_download_has_progress_without_total_deadline(monkeypatch,tmp_path):
    calls=[]
    monkeypatch.setattr(frp.subprocess,'run',lambda args,**kw:calls.append((args,kw)))
    frp.download_archive('https://github.com/example',tmp_path/'archive')
    args,opts=calls[0]
    assert '--progress-bar' in args and '--connect-timeout' in args
    assert '--max-time' not in args and '--retry-max-time' not in args
    assert 'timeout' not in opts and opts['check'] is True


def test_download_failure_reports_offline_alternative(monkeypatch,tmp_path):
    def fail(*a,**kw):raise subprocess.CalledProcessError(7,['curl'])
    monkeypatch.setattr(frp.subprocess,'run',fail)
    with pytest.raises(RuntimeError,match='--archive'):
        frp.download_archive('https://github.com/example',tmp_path/'archive')


def test_offline_archive_checksum_failure_never_installs(monkeypatch,tmp_path):
    monkeypatch.setattr(frp,'BIN',tmp_path/'bin')
    monkeypatch.setattr(frp.platform,'machine',lambda:'x86_64')
    def forbidden(*a):raise AssertionError('offline mode must not access network')
    monkeypatch.setattr(frp,'download_archive',forbidden)
    archive=tmp_path/'frp.tar.gz';archive.write_bytes(b'bad archive')
    with pytest.raises(ValueError,match='SHA256'):frp.install_binary('frps',archive)
    assert not (tmp_path/'bin').exists()
