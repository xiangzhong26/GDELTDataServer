"""Explicit local storage checks; never delete or repair existing databases."""
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile


def diagnose(settings):
    result = {'data_dir': str(settings.data_dir), 'checks': {}, 'ok': True}
    for label, folder in [('data_directory', settings.data_dir),
                          ('sqlite_temp_directory', Path(os.environ.get('SQLITE_TMPDIR', tempfile.gettempdir())))]:
        try:
            with tempfile.TemporaryFile(dir=folder) as f:
                f.write(b'gdelt storage check')
                f.flush()
                os.fsync(f.fileno())
            result['checks'][label] = {'ok': True, 'path': str(folder), 'free_bytes': shutil.disk_usage(folder).free}
        except OSError as exc:
            result['ok'] = False
            result['checks'][label] = {'ok': False, 'path': str(folder), 'error': str(exc)}
    path = settings.data_dir/'gdelt.db'
    try:
        db = sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True, timeout=10)
        try:
            rows = [r[0] for r in db.execute('PRAGMA quick_check')]
        finally:
            db.close()
        result['checks']['database'] = {'ok': rows == ['ok'], 'quick_check': rows}
        result['ok'] = result['ok'] and rows == ['ok']
    except sqlite3.Error as exc:
        result['ok'] = False
        result['checks']['database'] = {'ok': False, 'error': str(exc),
                                         'code': getattr(exc, 'sqlite_errorname', 'SQLITE_ERROR')}
    return result
