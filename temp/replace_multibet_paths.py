"""
Substitui paths hardcoded 'OneDrive - PGX/Projetos - Super Nova/MultiBet' por
'OneDrive - PGX/Projetos - Super Nova/MultiBet' em todos os arquivos
de codigo/docs. Executar ANTES da migracao pra deixar tudo pronto.

Substitui literal (sem regex), suporta forward e back slash.
"""
from pathlib import Path

BASE = Path(r"c:\Users\NITRO\OneDrive - PGX\Projetos - Super Nova\MultiBet")

SUBSTITUICOES = [
    # forward slash
    ("OneDrive - PGX/Projetos - Super Nova/MultiBet", "OneDrive - PGX/Projetos - Super Nova/MultiBet"),
    # back slash (single)
    ("OneDrive - PGX\\MultiBet", "OneDrive - PGX\\Projetos - Super Nova\\MultiBet"),
]

EXTENSOES = {".py", ".sh", ".sql", ".md", ".txt", ".json", ".yml", ".yaml"}

# Ignorar
SKIP_DIRS = {".git", "__pycache__", "venv", ".venv", "node_modules"}

alterados = []
examinados = 0
erros = []

for p in BASE.rglob("*"):
    if not p.is_file():
        continue
    if any(skip in p.parts for skip in SKIP_DIRS):
        continue
    if p.suffix.lower() not in EXTENSOES:
        continue
    examinados += 1
    try:
        conteudo = p.read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        erros.append((str(p), str(e)))
        continue

    novo = conteudo
    for antigo, novo_str in SUBSTITUICOES:
        novo = novo.replace(antigo, novo_str)

    if novo != conteudo:
        try:
            p.write_text(novo, encoding="utf-8")
            alterados.append(str(p.relative_to(BASE)))
        except Exception as e:
            erros.append((str(p), f"write: {e}"))

print(f"Arquivos examinados: {examinados:,}")
print(f"Arquivos alterados:  {len(alterados)}")
if erros:
    print(f"\nERROS: {len(erros)}")
    for path, err in erros[:5]:
        print(f"  {path}: {err}")

print(f"\nAlterados (top 20):")
for a in alterados[:20]:
    print(f"  {a}")
if len(alterados) > 20:
    print(f"  ... e mais {len(alterados) - 20}")
