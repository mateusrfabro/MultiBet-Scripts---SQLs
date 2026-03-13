# Recria a tarefa GrandesGanhos com:
#   - Execução mesmo sem bateria (com ou sem carregador)
#   - Execução mesmo com usuário deslogado (S4U)
#   - StartWhenAvailable: roda no próximo horário se o PC estava desligado/em sleep

$action = New-ScheduledTaskAction `
    -Execute "C:\Users\NITRO\OneDrive - PGX\MultiBet\pipelines\run_grandes_ganhos.bat"

$trigger = New-ScheduledTaskTrigger -Daily -At "00:30"

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Hours 1)

$principal = New-ScheduledTaskPrincipal `
    -UserId "NITRO" `
    -LogonType S4U `
    -RunLevel Highest

Register-ScheduledTask `
    -TaskName "GrandesGanhos" `
    -TaskPath "\MultiBet\" `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal `
    -Force

Write-Host "Tarefa criada com sucesso!"
