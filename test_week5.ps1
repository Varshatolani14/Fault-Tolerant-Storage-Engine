# Week 5 Replication Test Script
# Run from PowerShell: .\test_week5.ps1

Write-Host ""
Write-Host "================================================" -ForegroundColor Cyan
Write-Host "  Week 5 - Primary-Backup Replication Test" -ForegroundColor Cyan
Write-Host "================================================" -ForegroundColor Cyan

function Send-KV {
    param($writer, $reader, $cmd)
    $writer.WriteLine($cmd)
    Start-Sleep -Milliseconds 400
    $response = $reader.ReadLine()
    Write-Host "  SEND: $cmd" -ForegroundColor Yellow
    Write-Host "  RECV: $response" -ForegroundColor Green
    Write-Host ""
    return $response
}

# Test 1: Write to primary
Write-Host ""
Write-Host "--- Test 1: Writing to Primary port 9001 ---" -ForegroundColor White

try {
    $c1 = New-Object System.Net.Sockets.TcpClient('127.0.0.1', 9001)
    $s1 = $c1.GetStream()
    $w1 = New-Object System.IO.StreamWriter($s1)
    $r1 = New-Object System.IO.StreamReader($s1)
    $w1.AutoFlush = $true

    Send-KV $w1 $r1 "PING"
    Send-KV $w1 $r1 "SET city Chennai"
    Send-KV $w1 $r1 "SET lang C++"
    Send-KV $w1 $r1 "SET project KVStore"
    Send-KV $w1 $r1 "GET city"
    Send-KV $w1 $r1 "DEL lang"
    Send-KV $w1 $r1 "GET lang"

    $c1.Close()
    Write-Host "Primary test done." -ForegroundColor Green
} catch {
    Write-Host "ERROR: Could not connect to primary on port 9001." -ForegroundColor Red
    Write-Host "Make sure primary_node.exe is running!" -ForegroundColor Red
    exit
}

Start-Sleep -Seconds 1

# Test 2: Read from backup 9002
Write-Host ""
Write-Host "--- Test 2: Replication Check on Backup port 9002 ---" -ForegroundColor White

try {
    $c2 = New-Object System.Net.Sockets.TcpClient('127.0.0.1', 9002)
    $s2 = $c2.GetStream()
    $w2 = New-Object System.IO.StreamWriter($s2)
    $r2 = New-Object System.IO.StreamReader($s2)
    $w2.AutoFlush = $true

    $res1 = Send-KV $w2 $r2 "GET city"
    $res2 = Send-KV $w2 $r2 "GET lang"
    $res3 = Send-KV $w2 $r2 "GET project"
    $c2.Close()

    if ($res1 -eq "Chennai") {
        Write-Host "PASS: city=Chennai replicated to backup 9002" -ForegroundColor Green
    } else {
        Write-Host "FAIL: Expected Chennai, got $res1" -ForegroundColor Red
    }
    if ($res2 -eq "NOT_FOUND") {
        Write-Host "PASS: DEL propagated to backup 9002" -ForegroundColor Green
    } else {
        Write-Host "FAIL: Expected NOT_FOUND for lang, got $res2" -ForegroundColor Red
    }
} catch {
    Write-Host "ERROR: Could not connect to backup on port 9002." -ForegroundColor Red
}

# Test 3: Read from backup 9003
Write-Host ""
Write-Host "--- Test 3: Replication Check on Backup port 9003 ---" -ForegroundColor White

try {
    $c3 = New-Object System.Net.Sockets.TcpClient('127.0.0.1', 9003)
    $s3 = $c3.GetStream()
    $w3 = New-Object System.IO.StreamWriter($s3)
    $r3 = New-Object System.IO.StreamReader($s3)
    $w3.AutoFlush = $true

    $res4 = Send-KV $w3 $r3 "GET city"
    $res5 = Send-KV $w3 $r3 "GET project"
    $c3.Close()

    if ($res4 -eq "Chennai") {
        Write-Host "PASS: city=Chennai replicated to backup 9003" -ForegroundColor Green
    } else {
        Write-Host "FAIL: Expected Chennai, got $res4" -ForegroundColor Red
    }
} catch {
    Write-Host "ERROR: Could not connect to backup on port 9003." -ForegroundColor Red
}

# Test 4: WAL check
Write-Host ""
Write-Host "--- Test 4: WAL Persistence Check ---" -ForegroundColor White

$walPath = ".\kv_primary.log"
if (Test-Path $walPath) {
    $lines = Get-Content $walPath
    Write-Host "PASS: WAL file kv_primary.log exists" -ForegroundColor Green
    Write-Host "PASS: WAL contains $($lines.Count) entries:" -ForegroundColor Green
    foreach ($line in $lines) {
        Write-Host "  $line" -ForegroundColor DarkGray
    }
} else {
    Write-Host "FAIL: WAL file not found" -ForegroundColor Red
}

Write-Host ""
Write-Host "================================================" -ForegroundColor Cyan
Write-Host "  Test complete. All PASS = Week 5 working!" -ForegroundColor Cyan
Write-Host "================================================" -ForegroundColor Cyan
Write-Host ""
