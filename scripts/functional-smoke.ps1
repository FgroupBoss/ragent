param(
    [string]$Base = 'http://127.0.0.1:9090/api/ragent',
    [string]$Username = 'admin',
    [string]$Password = 'admin'
)

$ErrorActionPreference = 'Stop'
$pass = 0
$fail = 0

function Assert-Step {
    param([string]$Name, [bool]$Ok, [string]$Detail = '')
    if ($Ok) { $script:pass++; Write-Host "[PASS] $Name" -ForegroundColor Green }
    else { $script:fail++; Write-Host "[FAIL] $Name $Detail" -ForegroundColor Red }
}

Write-Host "=== Ragent 功能冒烟 ($Base) ===" -ForegroundColor Cyan

try {
    # 1. 登录
    $login = Invoke-RestMethod -Method Post -Uri "$Base/auth/login" -ContentType 'application/json' `
        -Body (@{ username = $Username; password = $Password } | ConvertTo-Json)
    Assert-Step '登录(admin)' ($login.code -eq '0' -and $login.data.role -eq 'admin') "code=$($login.code)"
    $headers = @{ Authorization = $login.data.token }

    # 2. 会话列表
    $convs = Invoke-RestMethod -Method Get -Uri "$Base/conversations" -Headers $headers
    Assert-Step '会话列表' ($convs.code -eq '0') "code=$($convs.code)"

    # 3. 示例问题
    $samples = Invoke-RestMethod -Method Get -Uri "$Base/rag/sample-questions" -Headers $headers
    Assert-Step '示例问题接口' ($samples.code -eq '0') "code=$($samples.code)"

    # 4. 创建知识库
    $ts = [DateTimeOffset]::Now.ToUnixTimeMilliseconds()
    $kbName = "smoke-$ts"
    $collection = "smoke_$ts"
    $kb = Invoke-RestMethod -Method Post -Uri "$Base/knowledge-base" -ContentType 'application/json' -Headers $headers `
        -Body (@{ name = $kbName; embeddingModel = 'qwen-emb-8b'; collectionName = $collection } | ConvertTo-Json)
    Assert-Step '创建知识库' ($kb.code -eq '0' -and $kb.data) "code=$($kb.code)"
    $kbId = $kb.data

    # 5. 上传文档
    $tmp = Join-Path $env:TEMP "ragent-smoke-$ts.md"
    Set-Content -Path $tmp -Value "# 冒烟文档`n这是一个功能测试文档，验证 MySQL+Kafka+MinIO 链路。" -Encoding UTF8
    $uploadJson = curl.exe -s -X POST -H "Authorization: $($headers.Authorization)" `
        -F 'sourceType=file' -F 'processMode=chunk' `
        -F "file=@$tmp;filename=smoke-$ts.md;type=text/markdown" `
        "$Base/knowledge-base/$kbId/docs/upload"
    $upDoc = $uploadJson | ConvertFrom-Json
    Assert-Step '上传文档(MinIO)' ($upDoc.code -eq '0' -and $upDoc.data.id) "code=$($upDoc.code)"
    $docId = $upDoc.data.id

    # 6. 触发分块（Kafka Outbox 事务）
    $chunkJson = curl.exe -s -X POST -H "Authorization: $($headers.Authorization)" "$Base/knowledge-base/docs/$docId/chunk"
    $chunk = $chunkJson | ConvertFrom-Json
    Assert-Step '触发分块(Outbox)' ($chunk.code -eq '0') "code=$($chunk.code)"

    # 7. 轮询文档状态（等待消费端执行）
    Start-Sleep -Seconds 20
    $doc = Invoke-RestMethod -Method Get -Uri "$Base/knowledge-base/docs/$docId" -Headers $headers
    $okStatus = $doc.data.status -in @('success', 'completed', 'failed')
    Assert-Step '分块状态流转' $okStatus "status=$($doc.data.status)"
    $logs = Invoke-RestMethod -Method Get -Uri "$Base/knowledge-base/docs/$docId/chunk-logs" -Headers $headers
    $records = @($logs.data.records)
    $logDetail = if ($records.Count -gt 0) { "status=$($records[0].status) parse=$($records[0].parseProfile)" } else { 'empty' }
    Assert-Step '分块日志落库' ($records.Count -gt 0) "count=$($records.Count) $logDetail"

    # 8. 清理：删文档 -> 删知识库（验证清理链路）
    $delDoc = Invoke-RestMethod -Method Delete -Uri "$Base/knowledge-base/docs/$docId" -Headers $headers
    Assert-Step '删除文档' ($delDoc.code -eq '0') "code=$($delDoc.code)"
    $delKb = Invoke-RestMethod -Method Delete -Uri "$Base/knowledge-base/$kbId" -Headers $headers
    Assert-Step '删除知识库(清理链路)' ($delKb.code -eq '0') "code=$($delKb.code)"
    Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue

    # 9. Trace 查询
    $trace = Invoke-RestMethod -Method Get -Uri "$Base/rag/traces/runs" -Headers $headers
    Assert-Step 'Trace 查询' ($trace.code -eq '0') "code=$($trace.code)"
}
catch {
    $script:fail++
    Write-Host "[FAIL] 异常: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host "=== 结果: PASS=$pass FAIL=$fail ===" -ForegroundColor Cyan
exit $(if ($fail -gt 0) { 1 } else { 0 })
