"""
Converte o schema_multibet_database_v1.0.md para PDF usando fpdf2.
Execucao: python scripts/md_to_pdf.py
"""
import re
from fpdf import FPDF

INPUT = "docs/schema_multibet_database_v1.0.md"
OUTPUT = "docs/schema_multibet_database_v1.1.pdf"


def sanitize(text):
    """Remove/substitui caracteres Unicode nao suportados pelo fpdf2 core fonts."""
    replacements = {
        "\u2014": "-",   # em-dash
        "\u2013": "-",   # en-dash
        "\u2018": "'",   # left single quote
        "\u2019": "'",   # right single quote
        "\u201c": '"',   # left double quote
        "\u201d": '"',   # right double quote
        "\u2026": "...", # ellipsis
        "\u2022": "-",   # bullet
        "\u2502": "|",   # box drawing
        "\u250c": "+",   # box corner
        "\u2510": "+",
        "\u2514": "+",
        "\u2518": "+",
        "\u251c": "+",
        "\u2524": "+",
        "\u252c": "+",
        "\u2534": "+",
        "\u253c": "+",
        "\u2500": "-",
        "\u2577": "|",
        "\u2576": "-",
        "\u25bc": "v",   # down triangle
        "\u25b6": ">",
        "\u2713": "[OK]",
        "\u2717": "[X]",
        "\u2610": "[ ]",
        "\u2611": "[x]",
        "\u2612": "[x]",
        "\u00e7": "c",   # ç
        "\u00e3": "a",   # ã
        "\u00e1": "a",   # á
        "\u00e9": "e",   # é
        "\u00ed": "i",   # í
        "\u00f3": "o",   # ó
        "\u00fa": "u",   # ú
        "\u00e2": "a",   # â
        "\u00ea": "e",   # ê
        "\u00f4": "o",   # ô
        "\u00e0": "a",   # à
        "\u00c7": "C",   # Ç
        "\u00c3": "A",   # Ã
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    # Remove any remaining non-latin1 characters
    return text.encode("latin-1", errors="replace").decode("latin-1")


class SchemaPDF(FPDF):
    def header(self):
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 5, sanitize("Schema MultiBet Database v1.1 - Super Nova Gaming"), align="R")
        self.ln(8)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f"Pagina {self.page_no()}/{{nb}}", align="C")

    def chapter_title(self, title, level=1):
        sizes = {1: 16, 2: 13, 3: 11, 4: 10}
        size = sizes.get(level, 10)
        self.set_font("Helvetica", "B", size)
        self.set_text_color(0, 51, 102)
        self.ln(4 if level > 1 else 8)
        self.multi_cell(0, size * 0.6, sanitize(title))
        if level <= 2:
            self.set_draw_color(0, 102, 204)
            self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())
        self.ln(3)

    def body_text(self, text):
        self.set_font("Helvetica", "", 9)
        self.set_text_color(30, 30, 30)
        self.multi_cell(0, 5, sanitize(text))
        self.ln(1)

    def bold_text(self, text):
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(30, 30, 30)
        self.multi_cell(0, 5, sanitize(text))
        self.ln(1)

    def code_block(self, text):
        self.set_font("Courier", "", 7.5)
        self.set_fill_color(245, 245, 245)
        self.set_text_color(50, 50, 50)
        for line in text.split("\n"):
            self.cell(0, 4, "  " + sanitize(line), fill=True, new_x="LMARGIN", new_y="NEXT")
        self.ln(2)

    def table_row(self, cells, header=False):
        self.set_font("Helvetica", "B" if header else "", 7.5)
        if header:
            self.set_fill_color(0, 51, 102)
            self.set_text_color(255, 255, 255)
        else:
            self.set_fill_color(250, 250, 250)
            self.set_text_color(30, 30, 30)

        # Calcular larguras proporcionais
        page_w = self.w - self.l_margin - self.r_margin
        n = len(cells)
        if n <= 2:
            widths = [page_w / n] * n
        elif n <= 4:
            widths = [page_w / n] * n
        else:
            # Primeira coluna mais larga, resto proporcional
            first_w = page_w * 0.22
            rest_w = (page_w - first_w) / (n - 1)
            widths = [first_w] + [rest_w] * (n - 1)

        h = 5
        for i, cell in enumerate(cells):
            w = widths[i] if i < len(widths) else widths[-1]
            txt = sanitize(str(cell).strip())
            # Truncar texto longo
            max_chars = int(w / 1.8)
            if len(txt) > max_chars:
                txt = txt[:max_chars - 2] + ".."
            self.cell(w, h, txt, border=1, fill=True)
        self.ln(h)


def parse_and_render(pdf, md_text):
    lines = md_text.split("\n")
    in_code = False
    code_buf = []
    in_table = False
    table_header_done = False
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
            # Skip separator rows
            if all(re.match(r'^[-:]+$', c) for c in cells):
                i += 1
                continue
            if not in_table:
                in_table = True
                table_header_done = False
                # Check if we need a page break
                if pdf.get_y() > pdf.h - 30:
                    pdf.add_page()
                pdf.table_row(cells, header=True)
                table_header_done = True
            else:
                # Alternate row colors
                pdf.table_row(cells, header=False)
            i += 1
            continue
        else:
            in_table = False
            table_header_done = False

        stripped = line.strip()

        # Empty lines
        if not stripped:
            i += 1
            continue

        # Horizontal rules
        if stripped == "---":
            pdf.ln(3)
            i += 1
            continue

        # Headers
        if stripped.startswith("#"):
            level = len(stripped) - len(stripped.lstrip("#"))
            title = stripped.lstrip("#").strip()
            pdf.chapter_title(title, level)
            i += 1
            continue

        # Bold lines (standalone **text**)
        if stripped.startswith("**") and stripped.endswith("**"):
            txt = stripped.strip("*")
            pdf.bold_text(txt)
            i += 1
            continue

        # Bold key-value (e.g. **Pipeline:** `...`)
        bold_match = re.match(r'^\*\*(.+?)\*\*\s*(.*)', stripped)
        if bold_match:
            key = bold_match.group(1)
            val = bold_match.group(2).replace("`", "")
            pdf.set_font("Helvetica", "B", 9)
            pdf.set_text_color(30, 30, 30)
            key_w = pdf.get_string_width(key + " ") + 2
            pdf.cell(key_w, 5, sanitize(key) + " ")
            pdf.set_font("Helvetica", "", 9)
            pdf.multi_cell(0, 5, sanitize(val))
            pdf.ln(1)
            i += 1
            continue

        # List items
        if stripped.startswith("- ") or stripped.startswith("* "):
            txt = stripped[2:]
            # Remove markdown formatting
            txt = re.sub(r'\*\*(.+?)\*\*', r'\1', txt)
            txt = re.sub(r'`(.+?)`', r'\1', txt)
            pdf.set_font("Helvetica", "", 8.5)
            pdf.set_text_color(30, 30, 30)
            pdf.cell(5, 4, "-")  # bullet
            pdf.multi_cell(0, 4, sanitize(txt))
            pdf.ln(0.5)
            i += 1
            continue

        # Regular text
        txt = re.sub(r'\*\*(.+?)\*\*', r'\1', stripped)
        txt = re.sub(r'`(.+?)`', r'\1', txt)
        pdf.body_text(sanitize(txt))
        i += 1


def main():
    with open(INPUT, "r", encoding="utf-8") as f:
        md = f.read()

    pdf = SchemaPDF(orientation="P", unit="mm", format="A4")
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()

    # Title page
    pdf.set_font("Helvetica", "B", 22)
    pdf.set_text_color(0, 51, 102)
    pdf.ln(30)
    pdf.cell(0, 12, sanitize("Schema do Banco de Dados MultiBet"), align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(5)
    pdf.set_font("Helvetica", "", 14)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(0, 8, sanitize("Super Nova DB (PostgreSQL - AWS RDS)"), align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)
    pdf.cell(0, 8, sanitize("Schema: multibet | Versao 1.1"), align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(15)
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 6, sanitize("Responsavel: Mateus Fabro - Squad Intelligence Engine"), align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 6, sanitize("Empresa: Super Nova Gaming"), align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 6, sanitize("Data: 18-19/03/2026 | Atualizado: 19/03/2026"), align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(20)
    pdf.set_draw_color(0, 102, 204)
    pdf.line(40, pdf.get_y(), pdf.w - 40, pdf.get_y())

    # Content
    pdf.add_page()
    parse_and_render(pdf, md)

    pdf.output(OUTPUT)
    print(f"PDF gerado: {OUTPUT}")
    print(f"Paginas: {pdf.page_no()}")


if __name__ == "__main__":
    main()
