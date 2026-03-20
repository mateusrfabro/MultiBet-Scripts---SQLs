# Data Intelligence | MultiBet & SuperNova

## Campanha de Reativação: One-Timers (Dez/25 - Jan/26)

### 1. Objetivo Técnico
Extração de base de contatos para campanha de Second Time Deposit (STD). O foco são usuários "One-Timers" que depositaram apenas uma vez entre Dezembro de 2025 e Janeiro de 2026.

### 2. Regras de Negócio & Lógica
- **Exclusividade**: Filtro MANDATÓRIO de COUNT(deposits) = 1 na história de vida do jogador.
- **Safra**: FTD capturado via c_created_time para precisão absoluta do período.
- **Blindagem**: Uso de Regex Hardcoded para garantir 100% de cobertura nos trackers.

### 3. Compliance & Qualidade (Match Rate)
- **Segurança**: Apenas usuários ativos, reais e com Opt-in de marketing (c_send_promotional_emails = 1).
- **Email**: Formatado com LOWER e TRIM para evitar erros de hash no Meta/Google.
- **Telefone**: Normalizado para padrão E.164 (+DDI+Número) com tratamento de campos nulos.

### 4. Protocolo de Exportação
1. Formato: CSV.
2. Delimitador: ";" (Ponto e Vírgula).
3. Numbers: Desmarcar "Formatar números".

---
**The Architect (MultiBet/SuperNova)**
