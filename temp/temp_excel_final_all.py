import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import openpyxl

wb = openpyxl.load_workbook('C:/Users/NITRO/Downloads/igaming_kpis_v2.xlsx')
ws = wb.active

updates = {
    67: "IMPLEMENTADA: COUNT(DISTINCT c_ecr_id) com janela deslizante. Avg DAU: 4.249, MAU: 36.435",
    68: "IMPLEMENTADA: = DAU. COUNT DISTINCT jogadores com bets no dia",
    69: "IMPLEMENTADA: DAU/MAU * 100 = 11.7% (abaixo de 20% = atencao)",
    70: "IMPLEMENTADA: total_bets / dau = avg_bets_per_player",
    72: "IMPLEMENTADA: total_ggr / dau = ggr_per_dau",
}

for row, text in updates.items():
    ws.cell(row=row, column=8, value=text)

wb.save('C:/Users/NITRO/Downloads/igaming_kpis_v2.xlsx')
print("Excel finalizado!")
