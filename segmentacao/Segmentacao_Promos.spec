# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['segmentacao_app\\app.py'],
    pathex=[],
    binaries=[],
    datas=[('db', 'db'), ('segmentacao_app', 'segmentacao_app'), ('.env', '.'), ('bigquery_credentials.json', '.')],
    hiddenimports=['redshift_connector', 'google.cloud.bigquery', 'google.oauth2.service_account', 'pandas', 'dotenv', 'segmentacao_app.parser', 'segmentacao_app.engine', 'segmentacao_app.game_catalog'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='Segmentacao_Promos',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='Segmentacao_Promos',
)
