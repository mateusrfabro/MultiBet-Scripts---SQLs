"""
Scraper: Captura de Jogos da MultiBet
======================================
Autor original: Gustavo (Analista Sênior - Infra)
Adaptado para integração com pipeline game_image_mapper.

Usa Playwright (headless Chromium) para navegar em https://multi.bet.br/pb/jogos,
coletar todas as seções de jogos e extrair nome + URL da imagem de cada jogo.

Gera arquivo jogos.csv na raiz do projeto.

Requisitos:
    pip install playwright
    playwright install chromium

Execução:
    python pipelines/capturar_jogos_pc.py
"""

from playwright.sync_api import sync_playwright
import csv
import time

URL_PRINCIPAL = "https://multi.bet.br/pb/jogos"
SECOES_IGNORAR = ["provedores de jogos", "providers", "game providers"]


def fechar_popups(page):
    for seletor in ["button[aria-label='Close']", "button[aria-label='Fechar']",
                    "[class*='modal-close']", "[class*='popup-close']",
                    "text=×", "text=✕"]:
        try:
            for el in page.query_selector_all(seletor):
                if el.is_visible():
                    el.click()
                    time.sleep(0.3)
        except:
            pass
    try:
        page.keyboard.press("Escape")
        time.sleep(0.3)
    except:
        pass


def scroll_completo(page):
    page.evaluate("window.scrollTo(0, 0)")
    time.sleep(0.3)
    while True:
        page.evaluate("window.scrollBy(0, 700)")
        time.sleep(0.35)
        if page.evaluate("window.scrollY + window.innerHeight >= document.body.scrollHeight"):
            break
    page.evaluate("window.scrollTo(0, 0)")
    time.sleep(0.5)


def coletar_urls_das_secoes(context):
    """
    Abre a página principal, clica em cada botão 'Veja mais' um por um,
    captura a URL de destino e volta. Retorna lista de {secao, url}.
    """
    page = context.new_page()
    page.on("dialog", lambda d: d.dismiss())
    page.goto(URL_PRINCIPAL, timeout=60000)
    page.wait_for_load_state("networkidle")
    time.sleep(2)
    fechar_popups(page)
    scroll_completo(page)
    time.sleep(1)

    # Primeiro coleta os nomes das seções em ordem
    secoes_nomes = page.evaluate("""
        (ignorar) => {
            const h2s = document.querySelectorAll('h2.lobby_group--title');
            return Array.from(h2s)
                .map(h => h.innerText.trim())
                .filter(n => !ignorar.some(i => n.toLowerCase().includes(i)));
        }
    """, SECOES_IGNORAR)

    print(f"  📂 {len(secoes_nomes)} seções encontradas: {', '.join(secoes_nomes)}\n")

    secoes_com_url = []

    for nome in secoes_nomes:
        try:
            # Recarrega a página principal para cada seção
            page.goto(URL_PRINCIPAL, timeout=30000)
            page.wait_for_load_state("networkidle")
            time.sleep(1.5)
            fechar_popups(page)
            scroll_completo(page)
            time.sleep(0.5)

            # Acha o botão "Veja mais" da seção pelo H2 correspondente
            clicou = page.evaluate("""
                (nomeSec) => {
                    const h2s = document.querySelectorAll('h2.lobby_group--title');
                    for (const h2 of h2s) {
                        if (h2.innerText.trim() !== nomeSec) continue;

                        // Sobe no DOM procurando o botão
                        let node = h2.parentElement;
                        for (let i = 0; i < 8; i++) {
                            if (!node) break;
                            // Botão pode ser irmão do h2 ou estar num wrapper
                            const btn = node.querySelector('.view-more-ct')
                                     || node.querySelector('button.lobby_group--button');
                            if (btn) {
                                btn.click();
                                return true;
                            }
                            // Também tenta em elementos irmãos
                            const parent = h2.parentElement;
                            if (parent) {
                                const irmao = parent.querySelector('.view-more-ct');
                                if (irmao) { irmao.click(); return true; }
                            }
                            node = node.parentElement;
                        }
                        return false;
                    }
                    return false;
                }
            """, nome)

            if clicou:
                # Espera a navegação acontecer
                try:
                    page.wait_for_url(lambda url: url != URL_PRINCIPAL, timeout=5000)
                    url_destino = page.url
                    print(f"    ✅ '{nome}' → {url_destino}")
                    secoes_com_url.append({"secao": nome, "url": url_destino})
                except:
                    # Navegação não ocorreu, pega URL atual
                    url_atual = page.url
                    if url_atual != URL_PRINCIPAL:
                        print(f"    ✅ '{nome}' → {url_atual}")
                        secoes_com_url.append({"secao": nome, "url": url_atual})
                    else:
                        print(f"    ⚠️  '{nome}' → clicou mas não navegou")
                        secoes_com_url.append({"secao": nome, "url": None})
            else:
                print(f"    ⭕️  '{nome}' → botão não encontrado")
                secoes_com_url.append({"secao": nome, "url": None})

        except Exception as e:
            print(f"    ❌ '{nome}' → erro: {e}")
            secoes_com_url.append({"secao": nome, "url": None})

    page.close()
    return secoes_com_url


def coletar_jogos_da_pagina(page):
    """Scroll completo + coleta todos os game-cards da página atual."""
    scroll_completo(page)
    fechar_popups(page)

    # Tenta clicar em "carregar mais" se existir
    for _ in range(30):
        botao = None
        for sel in [".view-more-ct", "[class*='load-more']", "[class*='show-more']",
                    "button:has-text('Carregar mais')", "button:has-text('Ver mais')"]:
            try:
                el = page.query_selector(sel)
                if el and el.is_visible():
                    botao = el
                    break
            except:
                pass
        if not botao:
            break
        botao.scroll_into_view_if_needed()
        botao.click()
        time.sleep(2)
        fechar_popups(page)
        try:
            page.wait_for_load_state("networkidle", timeout=5000)
        except:
            pass
        scroll_completo(page)

    jogos = page.evaluate("""
        () => {
            const resultado = [];
            const vistos = new Set();
            document.querySelectorAll('[class*="game-card"]').forEach(card => {
                const img  = card.querySelector('img');
                const h5   = card.querySelector('h5');
                const src  = img ? (img.src || img.getAttribute('data-src') || '') : '';
                const nome = h5  ? h5.innerText.trim() : (img ? img.alt : '');
                if (src && nome && src.startsWith('http') && !vistos.has(src)) {
                    vistos.add(src);
                    resultado.push({ nome: nome.trim(), url: src });
                }
            });
            return resultado;
        }
    """)
    return jogos


def main():
    print(f"🌐 Iniciando captura em: {URL_PRINCIPAL}\n")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            permissions=[],
            viewport={"width": 1440, "height": 900},
            locale="pt-BR",
        )

        # —— Passo 1: descobre as URLs de cada seção ——————————————————————————
        print("  🔍 Descobrindo URLs de cada seção...")
        secoes = coletar_urls_das_secoes(context)

        # —— Passo 2: visita cada URL e coleta os jogos ———————————————————————
        print("\n  🎮 Coletando jogos de cada seção...\n")
        resultado_secoes = []

        page = context.new_page()
        page.on("dialog", lambda d: d.dismiss())

        for s in secoes:
            nome = s["secao"]
            url  = s["url"]

            if not url:
                print(f"  ⭕️  '{nome}' → sem URL, pulando")
                resultado_secoes.append({"secao": nome, "jogos": []})
                continue

            try:
                page.goto(url, timeout=30000)
                page.wait_for_load_state("networkidle")
                time.sleep(2)
                fechar_popups(page)

                jogos = coletar_jogos_da_pagina(page)
                resultado_secoes.append({"secao": nome, "jogos": jogos})
                print(f"  ✅ '{nome}' → {len(jogos)} jogos  ({url})")
            except Exception as e:
                print(f"  ❌ '{nome}' → erro: {e}")
                resultado_secoes.append({"secao": nome, "jogos": []})

        page.close()
        context.close()
        browser.close()

    # —— CSV sem duplicatas ———————————————————————————————————————————————————
    vistos = set()
    todos_jogos = []
    for s in resultado_secoes:
        for j in s["jogos"]:
            if j["url"] not in vistos:
                vistos.add(j["url"])
                todos_jogos.append(j)

    arquivo_csv = "c:/Users/NITRO/OneDrive - PGX/MultiBet/pipelines/jogos.csv"
    with open(arquivo_csv, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["nome", "url"])
        writer.writeheader()
        writer.writerows(todos_jogos)

    # —— Relatório ————————————————————————————————————————————————————————————
    print(f"\n{'='*57}")
    print(f"  📊 RELATÓRIO FINAL")
    print(f"{'='*57}")
    print(f"  Jogos únicos salvos no CSV   : {len(todos_jogos)}")
    print(f"  Arquivo gerado               : {arquivo_csv}")
    print(f"\n  {'─'*53}")
    print(f"  {'SEÇÃO':<32} {'JOGOS':>6}")
    print(f"  {'─'*53}")

    total_soma = 0
    for s in resultado_secoes:
        qtd = len(s["jogos"])
        total_soma += qtd
        status = "✅" if qtd > 0 else "⚠️ "
        print(f"  {status}  {s['secao']:<32} {qtd:>5} jogos")

    print(f"  {'─'*53}")
    print(f"  {'Total (soma das seções)':<34} {total_soma:>5}")
    print(f"  {'Total único (sem duplicatas)':<34} {len(todos_jogos):>5}")
    print(f"{'='*57}")

    if todos_jogos:
        print("\n  Primeiros 5 resultados:")
        for j in todos_jogos[:5]:
            print(f"    🎮 {j['nome']:<35} → {j['url'][:60]}")


if __name__ == "__main__":
    main()
