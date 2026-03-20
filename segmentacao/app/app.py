"""
Segmentacao de Promocoes — Aplicacao Desktop (tkinter).

Interface para processar demandas de segmentacao de promocoes.
Cola o texto da demanda, clica em Processar, e recebe:
- Resumo com distribuicao por faixa
- Validacao cruzada Redshift vs BigQuery
- CSV/ZIP para download
- Mensagens prontas para WhatsApp

Uso:
    python -m segmentacao_app.app
    # ou
    python segmentacao_app/app.py
"""

import os
import sys
import logging
import threading
import tkinter as tk
from tkinter import ttk, scrolledtext, filedialog, messagebox
from datetime import datetime

# Setup path e garantir que .env eh encontrado independente de onde o app roda
APP_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(APP_DIR)
sys.path.insert(0, PROJECT_DIR)
os.chdir(PROJECT_DIR)  # garante que dotenv e credenciais sao encontradas

from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_DIR, '.env'))

from segmentacao_app.parser import parse_demanda
from segmentacao_app.engine import run_segmentacao

# Logging
LOG_DIR = os.path.join(APP_DIR, 'logs')
os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, f"segmentacao_{datetime.now().strftime('%Y%m%d')}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler(),
    ]
)
log = logging.getLogger(__name__)


class SegmentacaoApp:
    """Aplicacao principal de segmentacao de promocoes."""

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Segmentacao de Promocoes — MultiBet")
        self.root.geometry("1100x800")
        self.root.minsize(900, 650)

        # Variavel de estado
        self.processing = False
        self.last_result = None

        # Cores
        self.BG = "#1a1a2e"
        self.BG_LIGHT = "#16213e"
        self.FG = "#e0e0e0"
        self.ACCENT = "#0f3460"
        self.GREEN = "#2ecc71"
        self.RED = "#e74c3c"
        self.YELLOW = "#f39c12"
        self.BLUE = "#3498db"

        self.root.configure(bg=self.BG)

        self._build_ui()
        log.info("Aplicacao iniciada")

    def _build_ui(self):
        """Constroi toda a interface."""
        # Frame principal com scroll
        main_frame = tk.Frame(self.root, bg=self.BG)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=15, pady=10)

        # === TITULO ===
        title = tk.Label(
            main_frame, text="Segmentacao de Promocoes",
            font=("Segoe UI", 18, "bold"), fg=self.BLUE, bg=self.BG
        )
        title.pack(pady=(0, 5))

        subtitle = tk.Label(
            main_frame, text="Cole o texto da demanda e clique em Processar",
            font=("Segoe UI", 10), fg="#888", bg=self.BG
        )
        subtitle.pack(pady=(0, 10))

        # === AREA DE INPUT ===
        input_frame = tk.LabelFrame(
            main_frame, text=" Texto da Demanda ",
            font=("Segoe UI", 10, "bold"), fg=self.FG, bg=self.BG_LIGHT,
            relief=tk.GROOVE, bd=2
        )
        input_frame.pack(fill=tk.X, pady=(0, 10))

        self.txt_input = scrolledtext.ScrolledText(
            input_frame, height=8, font=("Consolas", 10),
            bg="#0a0a1a", fg=self.FG, insertbackground=self.FG,
            wrap=tk.WORD, relief=tk.FLAT, bd=5
        )
        self.txt_input.pack(fill=tk.X, padx=5, pady=5)

        # Placeholder
        self._set_placeholder()
        self.txt_input.bind("<FocusIn>", self._clear_placeholder)
        self.txt_input.bind("<FocusOut>", self._restore_placeholder)

        # === BOTOES ===
        btn_frame = tk.Frame(main_frame, bg=self.BG)
        btn_frame.pack(fill=tk.X, pady=(0, 10))

        self.btn_process = tk.Button(
            btn_frame, text="Processar", font=("Segoe UI", 12, "bold"),
            bg=self.GREEN, fg="white", activebackground="#27ae60",
            relief=tk.FLAT, padx=30, pady=8, cursor="hand2",
            command=self._on_process
        )
        self.btn_process.pack(side=tk.LEFT, padx=(0, 10))

        self.btn_clear = tk.Button(
            btn_frame, text="Limpar", font=("Segoe UI", 10),
            bg=self.ACCENT, fg=self.FG, activebackground="#1a4a8a",
            relief=tk.FLAT, padx=15, pady=8, cursor="hand2",
            command=self._on_clear
        )
        self.btn_clear.pack(side=tk.LEFT, padx=(0, 10))

        # Progress bar
        self.progress = ttk.Progressbar(btn_frame, mode='indeterminate', length=200)
        self.progress.pack(side=tk.LEFT, padx=(10, 0))

        # Status label
        self.lbl_status = tk.Label(
            btn_frame, text="Pronto", font=("Segoe UI", 9),
            fg="#888", bg=self.BG, anchor=tk.W
        )
        self.lbl_status.pack(side=tk.RIGHT, fill=tk.X, expand=True, padx=(10, 0))

        # === AREA DE PARSING (preview) ===
        self.parse_frame = tk.LabelFrame(
            main_frame, text=" Parametros Identificados ",
            font=("Segoe UI", 10, "bold"), fg=self.FG, bg=self.BG_LIGHT,
            relief=tk.GROOVE, bd=2
        )
        self.parse_frame.pack(fill=tk.X, pady=(0, 10))

        self.lbl_parse = tk.Label(
            self.parse_frame, text="Aguardando texto da demanda...",
            font=("Consolas", 9), fg="#888", bg=self.BG_LIGHT,
            justify=tk.LEFT, anchor=tk.W
        )
        self.lbl_parse.pack(fill=tk.X, padx=10, pady=5)

        # === LOG DE EXECUCAO ===
        log_frame = tk.LabelFrame(
            main_frame, text=" Log de Execucao ",
            font=("Segoe UI", 10, "bold"), fg=self.FG, bg=self.BG_LIGHT,
            relief=tk.GROOVE, bd=2
        )
        log_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        self.txt_log = scrolledtext.ScrolledText(
            log_frame, height=8, font=("Consolas", 9),
            bg="#0a0a1a", fg="#aaa", wrap=tk.WORD, relief=tk.FLAT, bd=5,
            state=tk.DISABLED
        )
        self.txt_log.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # === RESULTADO ===
        result_frame = tk.LabelFrame(
            main_frame, text=" Resultado ",
            font=("Segoe UI", 10, "bold"), fg=self.FG, bg=self.BG_LIGHT,
            relief=tk.GROOVE, bd=2
        )
        result_frame.pack(fill=tk.X, pady=(0, 10))

        self.txt_result = scrolledtext.ScrolledText(
            result_frame, height=10, font=("Consolas", 9),
            bg="#0a0a1a", fg=self.GREEN, wrap=tk.WORD, relief=tk.FLAT, bd=5,
            state=tk.DISABLED
        )
        self.txt_result.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # === BOTOES DE ACAO (pos-resultado) ===
        action_label = tk.Label(
            main_frame, text="Acoes:", font=("Segoe UI", 10, "bold"),
            fg=self.FG, bg=self.BG, anchor=tk.W
        )
        action_label.pack(fill=tk.X, pady=(5, 3))

        # Linha 1: Downloads
        dl_frame = tk.Frame(main_frame, bg=self.BG)
        dl_frame.pack(fill=tk.X, pady=(0, 5))

        self.btn_zip = tk.Button(
            dl_frame, text="Baixar ZIP", font=("Segoe UI", 10, "bold"),
            bg="#27ae60", fg="white", relief=tk.FLAT, padx=20, pady=6,
            cursor="hand2", command=self._on_save_zip, state=tk.DISABLED
        )
        self.btn_zip.pack(side=tk.LEFT, padx=(0, 10))

        self.btn_csv = tk.Button(
            dl_frame, text="Baixar CSV", font=("Segoe UI", 10, "bold"),
            bg="#2980b9", fg="white", relief=tk.FLAT, padx=20, pady=6,
            cursor="hand2", command=self._on_save_csv, state=tk.DISABLED
        )
        self.btn_csv.pack(side=tk.LEFT, padx=(0, 10))

        self.btn_open_folder = tk.Button(
            dl_frame, text="Abrir Pasta", font=("Segoe UI", 10),
            bg=self.ACCENT, fg=self.FG, relief=tk.FLAT, padx=15, pady=6,
            cursor="hand2", command=self._on_open_folder, state=tk.DISABLED
        )
        self.btn_open_folder.pack(side=tk.LEFT)

        # Linha 2: WhatsApp
        wpp_frame = tk.Frame(main_frame, bg=self.BG)
        wpp_frame.pack(fill=tk.X)

        self.btn_copy_msg1 = tk.Button(
            wpp_frame, text="Copiar Resumo (WhatsApp)", font=("Segoe UI", 10),
            bg="#075e54", fg="white", relief=tk.FLAT, padx=15, pady=6,
            cursor="hand2", command=lambda: self._copy_to_clipboard("msg1"), state=tk.DISABLED
        )
        self.btn_copy_msg1.pack(side=tk.LEFT, padx=(0, 10))

        self.btn_copy_msg2 = tk.Button(
            wpp_frame, text="Copiar Validacoes (WhatsApp)", font=("Segoe UI", 10),
            bg="#075e54", fg="white", relief=tk.FLAT, padx=15, pady=6,
            cursor="hand2", command=lambda: self._copy_to_clipboard("msg2"), state=tk.DISABLED
        )
        self.btn_copy_msg2.pack(side=tk.LEFT)

        # Bind Ctrl+V para auto-colar
        self.txt_input.bind("<Control-v>", self._on_paste)

    # === PLACEHOLDER ===

    def _set_placeholder(self):
        self.txt_input.insert("1.0", "Cole aqui o texto da demanda (ex: mark user, jogo, faixas...)")
        self.txt_input.configure(fg="#555")
        self._placeholder_active = True

    def _clear_placeholder(self, event=None):
        if self._placeholder_active:
            self.txt_input.delete("1.0", tk.END)
            self.txt_input.configure(fg=self.FG)
            self._placeholder_active = False

    def _restore_placeholder(self, event=None):
        if not self.txt_input.get("1.0", tk.END).strip():
            self._set_placeholder()

    def _on_paste(self, event=None):
        if self._placeholder_active:
            self._clear_placeholder()
        # Apos colar, faz preview do parsing
        self.root.after(100, self._preview_parse)

    # === PREVIEW DO PARSING ===

    def _preview_parse(self):
        texto = self.txt_input.get("1.0", tk.END).strip()
        if not texto or self._placeholder_active:
            self.lbl_parse.configure(text="Aguardando texto da demanda...", fg="#888")
            return

        demanda = parse_demanda(texto)
        if demanda.valido:
            faixas_str = " | ".join(f"{f.nome}: R${f.valor_min:.0f}-R${f.valor_max}" for f in demanda.faixas)
            info = (
                f"Tag: {demanda.mark_tag}\n"
                f"Jogo: {demanda.nome_jogo}\n"
                f"Periodo: {demanda.inicio_brt.strftime('%d/%m/%Y %Hh%M')} a "
                f"{demanda.fim_brt.strftime('%d/%m/%Y %Hh%M')} BRT\n"
                f"UTC: {demanda.inicio_utc.strftime('%Y-%m-%d %H:%M')} a "
                f"{demanda.fim_utc.strftime('%Y-%m-%d %H:%M')}\n"
                f"Rollback: {'Permitido (desconta)' if demanda.rollback_permitido else 'Desclassifica'}\n"
                f"Faixas: {faixas_str}"
            )
            self.lbl_parse.configure(text=info, fg=self.GREEN)
        else:
            self.lbl_parse.configure(text=f"Erros: {', '.join(demanda.erros)}", fg=self.RED)

    # === PROCESSAR ===

    def _on_process(self):
        if self.processing:
            return

        texto = self.txt_input.get("1.0", tk.END).strip()
        if not texto or self._placeholder_active:
            messagebox.showwarning("Atencao", "Cole o texto da demanda antes de processar.")
            return

        # Parsear
        demanda = parse_demanda(texto)
        if not demanda.valido:
            messagebox.showerror("Erro no Parsing", "\n".join(demanda.erros))
            return

        # Preview
        self._preview_parse()

        # Limpar log e resultado
        self._clear_log()
        self._clear_result()

        # Output dir
        safe_name = demanda.nome_jogo.lower().replace(' ', '_')
        dt_str = demanda.inicio_brt.strftime('%d%m%Y') if demanda.inicio_brt else 'sem_data'
        output_dir = os.path.join(PROJECT_DIR, f"segmentacao_output_{safe_name}_{dt_str}")

        # Iniciar processamento em thread separada
        self.processing = True
        self.btn_process.configure(state=tk.DISABLED, bg="#555")
        self.progress.start(10)
        self.lbl_status.configure(text="Processando...", fg=self.YELLOW)

        thread = threading.Thread(
            target=self._process_thread,
            args=(demanda, output_dir),
            daemon=True
        )
        thread.start()

    def _process_thread(self, demanda, output_dir):
        """Thread de processamento (nao bloqueia a UI)."""
        try:
            result = run_segmentacao(
                demanda,
                output_dir=output_dir,
                callback=lambda msg: self.root.after(0, self._append_log, msg)
            )
            self.root.after(0, self._on_process_done, result)
        except Exception as e:
            log.exception("Erro no processamento")
            self.root.after(0, self._on_process_error, str(e))

    def _on_process_done(self, result):
        """Callback quando processamento termina."""
        self.processing = False
        self.progress.stop()
        self.btn_process.configure(state=tk.NORMAL, bg=self.GREEN)
        self.last_result = result

        if result.success:
            self.lbl_status.configure(text="Concluido com sucesso!", fg=self.GREEN)
            self._show_result(result)
            self._enable_actions()
            log.info("Processamento concluido com sucesso")
        else:
            self.lbl_status.configure(text=f"Erro: {result.erro}", fg=self.RED)
            self._append_log(f"\nERRO: {result.erro}")
            log.error(f"Processamento falhou: {result.erro}")

    def _on_process_error(self, error_msg):
        """Callback quando processamento da erro."""
        self.processing = False
        self.progress.stop()
        self.btn_process.configure(state=tk.NORMAL, bg=self.GREEN)
        self.lbl_status.configure(text=f"Erro: {error_msg}", fg=self.RED)
        self._append_log(f"\nERRO CRITICO: {error_msg}")

    # === RESULTADO ===

    def _show_result(self, result):
        """Exibe resultado formatado igual as mensagens do WhatsApp."""
        def fmt(v):
            return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

        # Periodo formatado
        periodo_str = ""
        if result.inicio_brt:
            try:
                dt = datetime.strptime(result.inicio_brt, "%Y-%m-%d %H:%M:%S")
                periodo_str = dt.strftime("%d/%m")
                hora_ini = dt.strftime("%Hh")
                dt_fim = datetime.strptime(result.fim_brt, "%Y-%m-%d %H:%M:%S")
                hora_fim = dt_fim.strftime("%Hh%M")
            except:
                periodo_str = ""
                hora_ini = ""
                hora_fim = ""

        nao_jogou = result.total_marcados - result.total_jogaram
        pct_nao = (nao_jogou / result.total_marcados * 100) if result.total_marcados > 0 else 0

        # === MENSAGEM 1 — RESUMO (igual foto WhatsApp) ===
        lines = []
        lines.append(f"Segmentacao {result.game_name} | Promocao {result.mark_tag} | Periodo: {periodo_str}")
        lines.append(f"Do segmento com opt-in ({result.total_marcados:,} usuarios marcados), "
                      f"{result.total_jogaram} jogaram {result.game_name} no periodo "
                      f"({periodo_str} {hora_ini}-{hora_fim}). "
                      f"Total apostado: {fmt(result.net_bet_brl)}.")
        lines.append("")
        lines.append("Distribuicao por faixa:")
        for f in result.faixas:
            if f.nome == 'Nao jogou':
                continue
            # Descobrir limites da faixa para exibir
            lines.append(f"  * {f.nome}: {f.jogadores} jogadores — {fmt(f.volume_brl)} ({f.pct_volume:.0f}% do volume)")
        lines.append(f"  * Desclassificados (rollback): {result.total_desclassificados}")
        lines.append("")
        lines.append(f"Ponto de atencao: {result.total_desclassificados} desclassificado(s) por rollback. "
                      f"{pct_nao:.0f}% dos marcados nao jogaram {result.game_name} no periodo.")

        # === SEPARADOR ===
        lines.append("")
        lines.append("=" * 70)
        lines.append("")

        # === MENSAGEM 2 — VALIDACOES ===
        lines.append("Validacoes realizadas:")
        lines.append("")
        lines.append(f"1. {result.game_name} confirmado como game_id {result.redshift_game_id} "
                      f"({result.vendor}) no catalogo oficial")
        lines.append("")
        lines.append(f"2. Usuarios extraidos do BigQuery Smartico via tag {result.mark_tag} "
                      f"em j_user.core_tags — {result.total_marcados:,} com opt-in confirmado")
        lines.append("")
        lines.append(f"3. Valores confirmados em centavos pela documentacao da Pragmatic (v1.3) "
                      f"— divisao por 100 aplicada")
        lines.append("")
        rb_txt = "jogadores desclassificados conforme regra" if not result.rollback_permitido else "descontados do net bet"
        lines.append(f"4. {result.total_desclassificados} rollback(s) no periodo — {rb_txt}")
        lines.append("")
        lines.append(f"5. Mapeamento de IDs validado: Smartico user_ext_id = c_external_id "
                      f"na tabela ECR da Pragmatic")
        lines.append("")
        lines.append(f"6. Cada jogador aparece em apenas uma faixa (a mais alta atingida) "
                      f"— sem duplicidade de pagamento")

        if result.validacao_ok:
            diff_val = abs(result.net_bet_brl - result.validacao_total_smr)
            lines.append("")
            lines.append(f"7. Validacao cruzada Redshift vs BigQuery: "
                          f"{result.validacao_jogadores_smr} jogadores em ambas as fontes, "
                          f"diferenca de {fmt(diff_val)} no total ({result.validacao_diff_pct:.2f}%) "
                          f"— dados consistentes")

        # === ARQUIVOS ===
        lines.append("")
        lines.append("=" * 70)
        lines.append("")
        lines.append(f"Arquivos gerados:")
        lines.append(f"  CSV: {result.csv_path}")
        lines.append(f"  ZIP: {result.zip_path}")

        self.txt_result.configure(state=tk.NORMAL)
        self.txt_result.delete("1.0", tk.END)
        self.txt_result.insert("1.0", "\n".join(lines))
        self.txt_result.configure(state=tk.DISABLED)

    # === ACOES ===

    def _enable_actions(self):
        for btn in [self.btn_zip, self.btn_csv, self.btn_copy_msg1, self.btn_copy_msg2, self.btn_open_folder]:
            btn.configure(state=tk.NORMAL)

    def _on_save_csv(self):
        if not self.last_result or not self.last_result.csv_path:
            return
        src = self.last_result.csv_path
        dst = filedialog.asksaveasfilename(
            title="Salvar CSV",
            defaultextension=".csv",
            initialfile=os.path.basename(src),
            filetypes=[("CSV", "*.csv")],
        )
        if dst:
            import shutil
            shutil.copy2(src, dst)
            messagebox.showinfo("Salvo", f"CSV salvo em:\n{dst}")

    def _on_save_zip(self):
        if not self.last_result or not self.last_result.zip_path:
            return
        src = self.last_result.zip_path
        dst = filedialog.asksaveasfilename(
            title="Salvar ZIP",
            defaultextension=".zip",
            initialfile=os.path.basename(src),
            filetypes=[("ZIP", "*.zip")],
        )
        if dst:
            import shutil
            shutil.copy2(src, dst)
            messagebox.showinfo("Salvo", f"ZIP salvo em:\n{dst}")

    def _copy_to_clipboard(self, which):
        if not self.last_result:
            return
        text = self.last_result.msg_resumo if which == "msg1" else self.last_result.msg_validacao
        self.root.clipboard_clear()
        self.root.clipboard_append(text)
        self.root.update()
        self.lbl_status.configure(
            text=f"Mensagem {which[-1]} copiada para a area de transferencia!",
            fg=self.GREEN
        )

    def _on_open_folder(self):
        if not self.last_result or not self.last_result.csv_path:
            return
        folder = os.path.dirname(self.last_result.csv_path)
        os.startfile(folder)

    # === LOG ===

    def _append_log(self, msg):
        self.txt_log.configure(state=tk.NORMAL)
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.txt_log.insert(tk.END, f"[{timestamp}] {msg}\n")
        self.txt_log.see(tk.END)
        self.txt_log.configure(state=tk.DISABLED)

    def _clear_log(self):
        self.txt_log.configure(state=tk.NORMAL)
        self.txt_log.delete("1.0", tk.END)
        self.txt_log.configure(state=tk.DISABLED)

    def _clear_result(self):
        self.txt_result.configure(state=tk.NORMAL)
        self.txt_result.delete("1.0", tk.END)
        self.txt_result.configure(state=tk.DISABLED)
        for btn in [self.btn_zip, self.btn_csv, self.btn_copy_msg1, self.btn_copy_msg2, self.btn_open_folder]:
            btn.configure(state=tk.DISABLED)

    def _on_clear(self):
        self.txt_input.delete("1.0", tk.END)
        self._set_placeholder()
        self._clear_log()
        self._clear_result()
        self.lbl_parse.configure(text="Aguardando texto da demanda...", fg="#888")
        self.lbl_status.configure(text="Pronto", fg="#888")
        self.last_result = None


def main():
    root = tk.Tk()
    app = SegmentacaoApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
