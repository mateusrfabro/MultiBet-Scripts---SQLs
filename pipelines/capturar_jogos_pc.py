"""
Scraper: Captura de Jogos da MultiBet
======================================
Autor original: Gustavo (Analista Senior - Infra)
Adaptado para integracao com pipeline game_image_mapper.

Usa Playwright (headless Chromium) para navegar em https://multi.bet.br,
coletar todas as secoes de jogos (slots, live casino, jogos de mesa, etc.)
e extrair nome + URL da imagem de cada jogo.

Gera arquivo jogos.csv no diretorio pipelines/.

Requisitos:
    pip install playwright
    playwright install chromium

Execucao:
    python pipelines/capturar_jogos_pc.py
"""

from playwright.sync_api import sync_playwright
import csv
import time

# Paginas de entrada para varrer todas as categorias
URLS_ENTRADA = [
    "https://multi.bet.br/pb/jogos",
    "https://multi.bet.br/pb/jogosaovivo",
]

SECOES_IGNORAR = ["provedores de jogos", "providers", "game providers"]


def fechar_popups(page):
    for seletor in ["button[aria-label='Close']", "button[aria-label='Fechar']",
                    "[class*='modal-close']", "[class*='popup-close']",
                    "text=x", "text=X"]:
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


def descobrir_urls_navegacao(page):
    """Descobre URLs de categorias no menu de navegacao do site."""
    urls = page.evaluate("""
        () => {
            const links = new Set();
            document.querySelectorAll('a[href]').forEach(a => {
                const href = a.href;
                if (href.includes('/pb/') && !href.includes('/gameplay/')
                    && !href.includes('/login') && !href.includes('/register')
                    && !href.includes('/promotions') && !href.includes('/account')) {
                    links.add(href.split('?')[0].split('#')[0]);
                }
            });
            return Array.from(links);
        }
    """)
    return urls


def coletar_urls_das_secoes(context, url_entrada):
    """
    Abre uma pagina de entrada, clica em cada botao 'Veja mais' um por um,
    captura a URL de destino e volta. Retorna lista de {secao, url}.
    """
    page = context.new_page()
    page.on("dialog", lambda d: d.dismiss())

    try:
        page.goto(url_entrada, timeout=120000)
        page.wait_for_load_state("domcontentloaded")
    except Exception as e:
        print(f"    Falha ao acessar {url_entrada}: {e}")
        page.close()
        return []

    time.sleep(2)
    fechar_popups(page)
    scroll_completo(page)
    time.sleep(1)

    # Coleta os nomes das secoes em ordem
    secoes_nomes = page.evaluate("""
        (ignorar) => {
            const h2s = document.querySelectorAll('h2.lobby_group--title');
            return Array.from(h2s)
                .map(h => h.innerText.trim())
                .filter(n => !ignorar.some(i => n.toLowerCase().includes(i)));
        }
    """, SECOES_IGNORAR)

    print(f"  {len(secoes_nomes)} secoes em {url_entrada}: {', '.join(secoes_nomes[:5])}{'...' if len(secoes_nomes) > 5 else ''}")

    secoes_com_url = []

    for nome in secoes_nomes:
        try:
            page.goto(url_entrada, timeout=120000)
            page.wait_for_load_state("domcontentloaded")
            time.sleep(1.5)
            fechar_popups(page)
            scroll_completo(page)
            time.sleep(0.5)

            clicou = page.evaluate("""
                (nomeSec) => {
                    const h2s = document.querySelectorAll('h2.lobby_group--title');
                    for (const h2 of h2s) {
                        if (h2.innerText.trim() !== nomeSec) continue;
                        let node = h2.parentElement;
                        for (let i = 0; i < 8; i++) {
                            if (!node) break;
                            const btn = node.querySelector('.view-more-ct')
                                     || node.querySelector('button.lobby_group--button');
                            if (btn) { btn.click(); return true; }
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
                try:
                    page.wait_for_url(lambda url: url != url_entrada, timeout=5000)
                    url_destino = page.url
                    print(f"    OK '{nome}' -> {url_destino}")
                    secoes_com_url.append({"secao": nome, "url": url_destino})
                except:
                    url_atual = page.url
                    if url_atual != url_entrada:
                        print(f"    OK '{nome}' -> {url_atual}")
                        secoes_com_url.append({"secao": nome, "url": url_atual})
                    else:
                        print(f"    ?? '{nome}' -> clicou mas nao navegou")
                        secoes_com_url.append({"secao": nome, "url": None})
            else:
                print(f"    -- '{nome}' -> botao nao encontrado")
                secoes_com_url.append({"secao": nome, "url": None})

        except Exception as e:
            print(f"    ERRO '{nome}' -> {e}")
            secoes_com_url.append({"secao": nome, "url": None})

    page.close()
    return secoes_com_url


def coletar_jogos_da_pagina(page):
    """Scroll completo + coleta todos os game-cards da pagina atual."""
    scroll_completo(page)
    fechar_popups(page)

    # Clica em "carregar mais" se existir
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
    print(f"Iniciando captura multi-pagina...\n")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            permissions=[],
            viewport={"width": 1440, "height": 900},
            locale="pt-BR",
        )

        # Passo 1: Descobre URLs de navegacao a partir da pagina principal
        print("  Descobrindo URLs de navegacao do site...")
        page_nav = context.new_page()
        page_nav.on("dialog", lambda d: d.dismiss())
        try:
            page_nav.goto(URLS_ENTRADA[0], timeout=120000)
            page_nav.wait_for_load_state("domcontentloaded")
            time.sleep(2)
            urls_descobertas = descobrir_urls_navegacao(page_nav)
            print(f"  {len(urls_descobertas)} URLs encontradas na navegacao")
        except:
            urls_descobertas = []
        page_nav.close()

        # Junta URLs hardcoded + descobertas (sem duplicatas)
        todas_urls = list(dict.fromkeys(URLS_ENTRADA + list(urls_descobertas)))
        print(f"  {len(todas_urls)} URLs totais para varrer\n")

        # Passo 2: Descobre secoes de cada URL de entrada
        print("  Descobrindo secoes de cada pagina...")
        todas_secoes = []
        urls_visitadas = set()

        for url_base in todas_urls:
            secoes = coletar_urls_das_secoes(context, url_base)
            for s in secoes:
                if s["url"] and s["url"] not in urls_visitadas:
                    urls_visitadas.add(s["url"])
                    todas_secoes.append(s)

            # A propria pagina de entrada tambem pode ter jogos diretamente
            if url_base not in urls_visitadas:
                urls_visitadas.add(url_base)
                todas_secoes.append({"secao": url_base.split("/")[-1], "url": url_base})

        print(f"\n  {len(todas_secoes)} secoes unicas para varrer\n")

        # Passo 3: Visita cada URL e coleta jogos
        print("  Coletando jogos de cada secao...\n")
        resultado_secoes = []

        page = context.new_page()
        page.on("dialog", lambda d: d.dismiss())

        for s in todas_secoes:
            nome = s["secao"]
            url = s["url"]

            if not url:
                resultado_secoes.append({"secao": nome, "jogos": []})
                continue

            try:
                page.goto(url, timeout=120000)
                page.wait_for_load_state("domcontentloaded")
                time.sleep(2)
                fechar_popups(page)

                jogos = coletar_jogos_da_pagina(page)
                resultado_secoes.append({"secao": nome, "jogos": jogos})
                print(f"  OK '{nome}' -> {len(jogos)} jogos  ({url})")
            except Exception as e:
                print(f"  ERRO '{nome}' -> {e}")
                resultado_secoes.append({"secao": nome, "jogos": []})

        page.close()
        context.close()
        browser.close()

    # CSV sem duplicatas
    vistos = set()
    todos_jogos = []
    for s in resultado_secoes:
        for j in s["jogos"]:
            chave = j["nome"].upper().strip()
            if chave not in vistos:
                vistos.add(chave)
                todos_jogos.append(j)

    arquivo_csv = "c:/Users/NITRO/OneDrive - PGX/MultiBet/pipelines/jogos.csv"
    if not todos_jogos:
        print("\n  ATENCAO: 0 jogos capturados. CSV NAO foi sobrescrito.")
        return
    with open(arquivo_csv, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["nome", "url"])
        writer.writeheader()
        writer.writerows(todos_jogos)

    # Relatorio
    print(f"\n{'='*57}")
    print(f"  RELATORIO FINAL")
    print(f"{'='*57}")
    print(f"  Jogos unicos salvos no CSV   : {len(todos_jogos)}")
    print(f"  Arquivo gerado               : {arquivo_csv}")
    print(f"\n  {'-'*53}")
    print(f"  {'SECAO':<32} {'JOGOS':>6}")
    print(f"  {'-'*53}")

    total_soma = 0
    for s in resultado_secoes:
        qtd = len(s["jogos"])
        total_soma += qtd
        status = "OK" if qtd > 0 else "--"
        print(f"  {status}  {s['secao']:<32} {qtd:>5} jogos")

    print(f"  {'-'*53}")
    print(f"  {'Total (soma das secoes)':<34} {total_soma:>5}")
    print(f"  {'Total unico (sem duplicatas)':<34} {len(todos_jogos):>5}")
    print(f"{'='*57}")

    if todos_jogos:
        print("\n  Primeiros 5 resultados:")
        for j in todos_jogos[:5]:
            print(f"    {j['nome']:<35} -> {j['url'][:60]}")


if __name__ == "__main__":
    main()
