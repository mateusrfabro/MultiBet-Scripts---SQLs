"""
Converte bronze_selects_kpis_CORRIGIDO_v2.md para PDF.
Execucao: python scripts/bronze_to_pdf.py
"""
import re
from fpdf import FPDF

INPUT = "docs/bronze_selects_kpis_CORRIGIDO_v2.md"
OUTPUT = "docs/bronze_selects_kpis_v2.pdf"


def sanitize(text):
    """Remove/substitui caracteres Unicode nao suportados pelo fpdf2 core fonts."""
    replacements = {
        "\u2014": "-", "\u2013": "-", "\u2018": "'", "\u2019": "'",
        "\u201c": '"', "\u201d": '"', "\u2026": "...", "\u2022": "-",
        "\u2502": "|", "\u250c": "+", "\u2510": "+", "\u2514": "+",
        "\u2518": "+", "\u251c": "+", "\u2524": "+", "\u252c": "+",
        "\u2534": "+", "\u253c": "+", "\u2500": "-", "\u25bc": "v",
        "\u25b6": ">", "\u2713": "[OK]", "\u2717": "[X]",
        "\u00e7": "c", "\u00e3": "a", "\u00e1": "a", "\u00e9": "e",
        "\u00ed": "i", "\u00f3": "o", "\u00fa": "u", "\u00e2": "a",
        "\u00ea": "e", "\u00f4": "o", "\u00e0": "a", "\u00c7": "C",
        "\u00c3": "A", "\u00c9": "E", "\u00cd": "I", "\u00d3": "O",
        "\u00da": "U",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text.encode("latin-1", errors="replace").decode("latin-1")


class BronzePDF(FPDF):
    def header(self):
        self.set_font("Helvetica", "I", 7)
        self.set_text_color(128, 128, 128)
        self.cell(0, 5, sanitize("Bronze SELECTs KPIs CORRIGIDO v2 - Super Nova Gaming"), align="R")
        self.ln(7)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f"Pagina {self.page_no()}/{{nb}}", align="C")

    def chapter_title(self, title, level=1):
        sizes = {1: 15, 2: 12, 3: 10, 4: 9}
        size = sizes.get(level, 9)
        self.set_font("Helvetica", "B", size)
        self.set_text_color(0, 51, 102)
        self.ln(3 if level > 2 else 6)
        self.multi_cell(0, size * 0.55, sanitize(title))
        if level <= 2:
            self.set_draw_color(0, 102, 204)
            self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())
        self.ln(2)

    def body_text(self, text):
        self.set_font("Helvetica", "", 8.5)
        self.set_text_color(30, 30, 30)
        self.multi_cell(0, 4.5, sanitize(text))
        self.ln(1)

    def bold_text(self, text):
        self.set_font("Helvetica", "B", 8.5)
        self.set_text_color(30, 30, 30)
        self.multi_cell(0, 4.5, sanitize(text))
        self.ln(1)

    def code_block(self, text):
        self.set_font("Courier", "", 7)
        self.set_fill_color(242, 242, 242)
        self.set_text_color(50, 50, 50)
        for line in text.split("\n"):
            if self.get_y() > self.h - 22:
                self.add_page()
            self.cell(0, 3.8, "  " + sanitize(line), fill=True, new_x="LMARGIN", new_y="NEXT")
        self.ln(2)

    def blockquote(self, text):
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(80, 80, 80)
        self.set_fill_color(255, 250, 230)
        x = self.get_x()
        self.set_draw_color(255, 180, 0)
        self.line(self.l_margin, self.get_y(), self.l_margin, self.get_y() + 8)
        self.set_x(self.l_margin + 4)
        self.multi_cell(self.w - self.l_margin - self.r_margin - 4, 4, sanitize(text))
        self.ln(2)

    def table_row(self, cells, header=False):
        self.set_font("Helvetica", "B" if header else "", 7)
        if header:
            self.set_fill_color(0, 51, 102)
            self.set_text_color(255, 255, 255)
        else:
            self.set_fill_color(250, 250, 250)
            self.set_text_color(30, 30, 30)

        page_w = self.w - self.l_margin - self.r_margin
        n = len(cells)
        if n == 0:
            return
        # Adaptive widths
        if n <= 2:
            widths = [page_w / n] * n
        elif n == 3:
            widths = [page_w * 0.30, page_w * 0.15, page_w * 0.55]
        elif n == 4:
            widths = [page_w * 0.05, page_w * 0.30, page_w * 0.25, page_w * 0.40]
        else:
            first_w = page_w * 0.20
            rest_w = (page_w - first_w) / (n - 1)
            widths = [first_w] + [rest_w] * (n - 1)

        h = 4.5
        if self.get_y() > self.h - 22:
            self.add_page()
        for i, cell in enumerate(cells):
            w = widths[i] if i < len(widths) else widths[-1]
            txt = sanitize(str(cell).strip())
            max_chars = int(w / 1.6)
            if len(txt) > max_chars:
                txt = txt[:max_chars - 2] + ".."
            self.cell(w, h, txt, border=1, fill=True)
        self.ln(h)


def parse_and_render(pdf, md_text):
    lines = md_text.split("\n")
    in_code = False
    code_buf = []
    in_table = False
    i = 0

    while i < len(lines):
        line = lines[i]

        # Code blocks
        if line.strip().startswith("```"):
            if in_code:
                pdf.code_block("\n".join(code_buf))
                code_buf = []
                in_code = False
            else:
                in_code = True
            i += 1
            continue

        if in_code:
            code_buf.append(line)
            i += 1
            continue

        # Tables
        if "|" in line and line.strip().startswith("|"):
            cells = [c.strip() for c in line.split("|")[1:-1]]
            if all(re.match(r'^[-:]+$', c) for c in cells):
                i += 1
                continue
            if not in_table:
                in_table = True
                if pdf.get_y() > pdf.h - 30:
                    pdf.add_page()
                pdf.table_row(cells, header=True)
            else:
                pdf.table_row(cells, header=False)
            i += 1
            continue
        else:
            in_table = False

        stripped = line.strip()

        if not stripped:
            i += 1
            continue

        if stripped == "---":
            pdf.ln(2)
            i += 1
            continue

        # Blockquotes
        if stripped.startswith("> "):
            txt = stripped[2:]
            txt = re.sub(r'\*\*(.+?)\*\*', r'\1', txt)
            txt = re.sub(r'`(.+?)`', r'\1', txt)
            pdf.blockquote(txt)
            i += 1
            continue

        # Headers
        if stripped.startswith("#"):
            level = len(stripped) - len(stripped.lstrip("#"))
            title = stripped.lstrip("#").strip()
            pdf.chapter_title(title, level)
            i += 1
            continue

        # Bold standalone
        if stripped.startswith("**") and stripped.endswith("**"):
            txt = stripped.strip("*")
            pdf.bold_text(txt)
            i += 1
            continue

        # Bold key-value
        bold_match = re.match(r'^\*\*(.+?)\*\*\s*(.*)', stripped)
        if bold_match:
            key = bold_match.group(1)
            val = bold_match.group(2).replace("`", "")
            pdf.set_font("Helvetica", "B", 8.5)
            pdf.set_text_color(30, 30, 30)
            key_w = pdf.get_string_width(key + " ") + 2
            pdf.cell(key_w, 4.5, sanitize(key) + " ")
            pdf.set_font("Helvetica", "", 8.5)
            pdf.multi_cell(0, 4.5, sanitize(val))
            pdf.ln(1)
            i += 1
            continue

        # List items
        if stripped.startswith("- ") or stripped.startswith("* "):
            txt = stripped[2:]
            txt = re.sub(r'\*\*(.+?)\*\*', r'\1', txt)
            txt = re.sub(r'`(.+?)`', r'\1', txt)
            pdf.set_font("Helvetica", "", 8)
            pdf.set_text_color(30, 30, 30)
            pdf.cell(4, 4, "-")
            pdf.multi_cell(0, 4, sanitize(txt))
            pdf.ln(0.5)
            i += 1
            continue

        # Regular text
        txt = re.sub(r'\*\*(.+?)\*\*', r'\1', stripped)
        txt = re.sub(r'`(.+?)`', r'\1', txt)
        pdf.body_text(txt)
        i += 1


def main():
    with open(INPUT, "r", encoding="utf-8") as f:
        md = f.read()

    pdf = BronzePDF(orientation="P", unit="mm", format="A4")
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=18)
    pdf.add_page()

    # Title page
    pdf.set_font("Helvetica", "B", 20)
    pdf.set_text_color(0, 51, 102)
    pdf.ln(25)
    pdf.cell(0, 10, sanitize("Mapeamento Bronze"), align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 10, sanitize("SELECTs para KPIs"), align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(5)
    pdf.set_font("Helvetica", "B", 14)
    pdf.set_text_color(0, 102, 50)
    pdf.cell(0, 8, sanitize("Versao 2.1 - Validado"), align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(5)
    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(0, 7, sanitize("ETL SuperNovaDB - Colunas Necessarias"), align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(10)
    pdf.set_font("Helvetica", "", 9)
    pdf.cell(0, 5, sanitize("Versao: 2.0 - Corrigido apos validacao empirica no Athena (SHOW COLUMNS)"), align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 5, sanitize("Validado por: Mateus Fabro - 2026-03-20"), align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 5, sanitize("Base: bronze_selects_kpis_FINAL.pdf (Mauro)"), align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 5, sanitize("21 tabelas Bronze (19 _ec2 brutos + 2 dimensoes ps_bi)"), align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 5, sanitize("Principio: Bronze = dados brutos. Calculos na Silver."), align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 5, sanitize("Todas as colunas validadas via SHOW COLUMNS + SELECT no Athena"), align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 5, sanitize("Empresa: Super Nova Gaming"), align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(15)
    pdf.set_draw_color(0, 102, 204)
    pdf.line(40, pdf.get_y(), pdf.w - 40, pdf.get_y())

    # Content - skip the header lines (already on title page)
    # Remove the first few lines that are metadata
    content_lines = md.split("\n")
    skip_until = 0
    for idx, line in enumerate(content_lines):
        if line.strip().startswith("## DOMINIO:"):
            skip_until = idx
            break
    content = "\n".join(content_lines[skip_until:])

    pdf.add_page()
    parse_and_render(pdf, content)

    pdf.output(OUTPUT)
    print(f"PDF gerado: {OUTPUT}")
    print(f"Paginas: {pdf.page_no()}")


if __name__ == "__main__":
    main()
