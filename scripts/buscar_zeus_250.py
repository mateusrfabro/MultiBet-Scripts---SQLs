"""
Busca direcionada: Zeus vs Hades Gods of War 250 no site multi.bet.br
Usa Playwright para encontrar o jogo e capturar a URL da imagem.
"""
from playwright.sync_api import sync_playwright
import time
import json

URL_BUSCA = "https://multi.bet.br/pb/jogos"

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1440, "height": 900},
            locale="pt-BR",
        )
        page = context.new_page()
        page.on("dialog", lambda d: d.dismiss())

        print("1. Abrindo site...")
        page.goto(URL_BUSCA, timeout=60000)
        page.wait_for_load_state("networkidle")
        time.sleep(3)

        # Fecha popups
        page.keyboard.press("Escape")
        time.sleep(0.5)

        # Busca pelo campo de search
        print("2. Procurando campo de busca...")
        search_selectors = [
            "input[type='search']",
            "input[placeholder*='Buscar']",
            "input[placeholder*='buscar']",
            "input[placeholder*='Search']",
            "input[placeholder*='search']",
            "input[placeholder*='Pesquisar']",
            ".search-input",
            "[class*='search'] input",
            "input[name='search']",
        ]

        search_input = None
        for sel in search_selectors:
            try:
                el = page.query_selector(sel)
                if el:
                    search_input = el
                    print(f"   Encontrou: {sel}")
                    break
            except:
                pass

        if not search_input:
            # Tenta clicar em icone de busca primeiro
            print("   Tentando clicar em icone de busca...")
            for sel in ["[class*='search']", "button[aria-label*='search']",
                        "button[aria-label*='Search']", ".search-icon", "[class*='Search']"]:
                try:
                    el = page.query_selector(sel)
                    if el and el.is_visible():
                        el.click()
                        time.sleep(1)
                        print(f"   Clicou em: {sel}")
                        break
                except:
                    pass

            # Tenta novamente
            for sel in search_selectors + ["input"]:
                try:
                    els = page.query_selector_all(sel)
                    for el in els:
                        if el.is_visible():
                            search_input = el
                            print(f"   Encontrou apos click: {sel}")
                            break
                    if search_input:
                        break
                except:
                    pass

        if search_input:
            print("3. Buscando 'zeus hades'...")
            search_input.click()
            search_input.fill("zeus")
            time.sleep(2)
            page.wait_for_load_state("networkidle")
            time.sleep(2)
        else:
            print("3. Campo de busca nao encontrado, scrollando toda a pagina...")

        # Scroll para carregar jogos
        for i in range(5):
            page.evaluate("window.scrollBy(0, 700)")
            time.sleep(0.5)
        page.evaluate("window.scrollTo(0, 0)")
        time.sleep(1)

        # Coleta TODOS os game-cards visiveis
        print("4. Coletando game-cards...")
        jogos = page.evaluate("""
            () => {
                const resultado = [];
                // Tenta varios seletores de game cards
                const seletores = [
                    '[class*="game-card"]',
                    '[class*="game_card"]',
                    '[class*="gameCard"]',
                    '.game-item',
                    '[class*="lobby"] [class*="game"]'
                ];

                for (const sel of seletores) {
                    document.querySelectorAll(sel).forEach(card => {
                        const img  = card.querySelector('img');
                        const h5   = card.querySelector('h5') || card.querySelector('[class*="name"]') || card.querySelector('span');
                        const src  = img ? (img.src || img.getAttribute('data-src') || '') : '';
                        const nome = h5 ? h5.innerText.trim() : (img ? (img.alt || '') : '');
                        if (nome) {
                            resultado.push({ nome, url: src });
                        }
                    });
                }
                return resultado;
            }
        """)

        # Filtra por Zeus/Hades/Gods
        print(f"\n   Total game-cards encontrados: {len(jogos)}")

        zeus_games = [j for j in jogos if any(k in j['nome'].lower() for k in ['zeus', 'hades', 'gods of war'])]

        print(f"\n5. Jogos relacionados a Zeus/Hades:")
        if zeus_games:
            for j in zeus_games:
                print(f"   -> {j['nome']}")
                print(f"      URL: {j['url']}")
        else:
            print("   Nenhum encontrado com busca. Listando todos para debug:")
            for j in jogos[:30]:
                print(f"   -> {j['nome']} | {j['url'][:80] if j['url'] else 'SEM URL'}")

        # Busca alternativa: procura TODAS as imagens da pagina
        print("\n6. Buscando todas as imagens com 'godsofwar' ou 'zeus' na URL...")
        all_imgs = page.evaluate("""
            () => {
                const imgs = [];
                document.querySelectorAll('img').forEach(img => {
                    const src = img.src || img.getAttribute('data-src') || '';
                    const alt = img.alt || '';
                    if (src.toLowerCase().includes('godsofwar') ||
                        src.toLowerCase().includes('zeus') ||
                        alt.toLowerCase().includes('zeus') ||
                        alt.toLowerCase().includes('gods of war')) {
                        imgs.push({alt, src});
                    }
                });
                return imgs;
            }
        """)

        if all_imgs:
            for img in all_imgs:
                print(f"   -> alt: {img['alt']}")
                print(f"      src: {img['src']}")
        else:
            print("   Nenhuma imagem encontrada com esses termos")

        # Busca 250 especificamente
        print("\n7. Buscando QUALQUER imagem com '250' na URL...")
        imgs_250 = page.evaluate("""
            () => {
                const imgs = [];
                document.querySelectorAll('img').forEach(img => {
                    const src = img.src || img.getAttribute('data-src') || '';
                    if (src.includes('250') || src.includes('gods')) {
                        imgs.push({alt: img.alt || '', src});
                    }
                });
                return imgs;
            }
        """)
        for img in imgs_250[:10]:
            print(f"   -> {img['alt']} | {img['src'][:100]}")

        # Tenta busca especifica por Pragmatic Play
        print("\n8. Navegando para secao Pragmatic Play...")
        try:
            page.goto("https://multi.bet.br/pb/jogos/pragmaticplay", timeout=30000)
            page.wait_for_load_state("networkidle")
            time.sleep(3)

            # Scroll para carregar
            for i in range(20):
                page.evaluate("window.scrollBy(0, 700)")
                time.sleep(0.4)

            zeus_pp = page.evaluate("""
                () => {
                    const resultado = [];
                    document.querySelectorAll('[class*="game-card"]').forEach(card => {
                        const img  = card.querySelector('img');
                        const h5   = card.querySelector('h5');
                        const src  = img ? (img.src || img.getAttribute('data-src') || '') : '';
                        const nome = h5 ? h5.innerText.trim() : (img ? img.alt : '');
                        if (nome && (nome.toLowerCase().includes('zeus') ||
                                     nome.toLowerCase().includes('gods') ||
                                     nome.toLowerCase().includes('hades') ||
                                     (src && src.includes('godsofwar')))) {
                            resultado.push({ nome, url: src });
                        }
                    });
                    return resultado;
                }
            """)

            print(f"   Jogos Zeus/Hades na Pragmatic Play: {len(zeus_pp)}")
            for j in zeus_pp:
                print(f"   -> {j['nome']}")
                print(f"      URL: {j['url']}")

            # Conta total de jogos PP
            total_pp = page.evaluate("document.querySelectorAll('[class*=\"game-card\"]').length")
            print(f"   Total jogos Pragmatic Play visiveis: {total_pp}")

        except Exception as e:
            print(f"   Erro: {e}")

        browser.close()

    print("\nDone!")

if __name__ == "__main__":
    main()
