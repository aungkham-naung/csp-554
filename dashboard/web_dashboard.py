import os

import boto3
from boto3.dynamodb.conditions import Key
from flask import Flask, jsonify, render_template_string

REGION = os.getenv("AWS_REGION", "us-east-2")
TABLE = os.getenv("DYNAMODB_TABLE", "CryptoMetrics")
SYMBOLS = ["BTCUSD", "ETHUSD", "SOLUSD"]

app = Flask(__name__)
dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table(TABLE)


@app.route("/api/symbols")
def get_symbols():
    return jsonify({"symbols": SYMBOLS})


@app.route("/api/ohlc/<symbol>")
def get_ohlc(symbol):
    symbol = symbol.upper()
    resp = table.query(
        KeyConditionExpression=Key("symbol").eq(symbol),
        ScanIndexForward=True,
        Limit=60,
    )
    items = [
        {
            "window_start": i["window_start"],
            "open": float(i.get("open", 0)),
            "high": float(i.get("high", 0)),
            "low": float(i.get("low", 0)),
            "close": float(i.get("close", 0)),
            "volume": float(i.get("volume", 0)),
            "vwap": float(i.get("vwap", 0)),
            "trade_count": int(i.get("trade_count", 0)),
        }
        for i in resp.get("Items", [])
    ]
    return jsonify({"symbol": symbol, "data": items})


HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>CSP 544 Crypto Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0"></script>
    <style>
        *{margin:0;padding:0;box-sizing:border-box}
        body{font-family:'Segoe UI',system-ui,sans-serif;background:#0d1117;color:#e6edf3;padding:24px}
        h1{font-size:20px;margin-bottom:4px}
        .sub{color:#8b949e;font-size:13px;margin-bottom:20px}
        .btns{display:flex;gap:8px;margin-bottom:20px}
        .btns button{padding:8px 16px;border:1px solid #30363d;background:#161b22;color:#e6edf3;
            border-radius:6px;cursor:pointer;font-size:13px}
        .btns button.on{background:#1f6feb;border-color:#1f6feb}
        .box{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:20px;margin-bottom:16px}
        .stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:12px;margin-bottom:20px}
        .s{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:14px}
        .sl{font-size:11px;color:#8b949e;text-transform:uppercase}
        .sv{font-size:22px;font-weight:600;margin-top:4px}
        .g{color:#3fb950}.r{color:#f85149}
        canvas{max-height:280px}
    </style>
</head>
<body>
    <h1>Crypto Pipeline Dashboard</h1>
    <p class="sub">CSP 544 | Coinbase &rarr; Kafka (MSK) &rarr; Spark (EMR) &rarr; DynamoDB &rarr; Chart.js</p>
    <div class="btns" id="btns"></div>
    <div class="stats" id="stats"></div>
    <div class="box"><canvas id="pc"></canvas></div>
    <div class="box"><canvas id="vc"></canvas></div>
<script>
let cur='BTCUSD',pC,vC;
async function init(){
    const{symbols}=await(await fetch('/api/symbols')).json();
    cur=symbols[0];
    const c=document.getElementById('btns');
    symbols.forEach(s=>{const b=document.createElement('button');
        b.textContent=s.replace('USD','');b.onclick=()=>{cur=s;go()};c.appendChild(b)});
    go();setInterval(go,15000)}
function stats(d){
    const e=document.getElementById('stats');
    if(!d.length){e.innerHTML='<p style="color:#8b949e">Waiting for data...</p>';return}
    const l=d[d.length-1],ch=l.open?((l.close-l.open)/l.open*100):0,
        cl=ch>=0?'g':'r',sg=ch>=0?'+':'';
    e.innerHTML=[['Close','$'+l.close.toFixed(2)],['VWAP','$'+l.vwap.toFixed(2)],
        ['Change',`<span class="${cl}">${sg}${ch.toFixed(3)}%</span>`],
        ['High','$'+l.high.toFixed(2)],['Low','$'+l.low.toFixed(2)],
        ['Volume',l.volume.toFixed(4)],['Trades',l.trade_count]]
        .map(([a,b])=>`<div class="s"><div class="sl">${a}</div><div class="sv">${b}</div></div>`).join('')}
function charts(d){
    const lb=d.map(x=>{const t=new Date(x.window_start);
        return t.toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'})}),
        cl=d.map(x=>x.close),vw=d.map(x=>x.vwap),
        vo=d.map(x=>x.volume),co=d.map(x=>x.close>=x.open?'#3fb950':'#f85149');
    if(pC)pC.destroy();
    pC=new Chart(document.getElementById('pc'),{type:'line',data:{labels:lb,datasets:[
        {label:'Close',data:cl,borderColor:'#58a6ff',backgroundColor:'rgba(88,166,255,0.1)',
         fill:true,tension:.3,pointRadius:2},
        {label:'VWAP',data:vw,borderColor:'#f0883e',borderDash:[5,3],pointRadius:0,tension:.3}]},
        options:{responsive:true,plugins:{legend:{labels:{color:'#8b949e'}},
        title:{display:true,text:cur+' Price',color:'#e6edf3'}},
        scales:{x:{ticks:{color:'#8b949e'},grid:{color:'#21262d'}},
        y:{ticks:{color:'#8b949e'},grid:{color:'#21262d'}}}}});
    if(vC)vC.destroy();
    vC=new Chart(document.getElementById('vc'),{type:'bar',data:{labels:lb,datasets:[
        {label:'Volume',data:vo,backgroundColor:co,borderRadius:2}]},
        options:{responsive:true,plugins:{legend:{display:false},
        title:{display:true,text:'Volume',color:'#e6edf3'}},
        scales:{x:{ticks:{color:'#8b949e'},grid:{color:'#21262d'}},
        y:{ticks:{color:'#8b949e'},grid:{color:'#21262d'}}}}})}
async function go(){
    document.querySelectorAll('.btns button').forEach(b=>
        b.classList.toggle('on',b.textContent===cur.replace('USD','')));
    const{data}=await(await fetch('/api/ohlc/'+cur)).json();
    stats(data);charts(data)}
init();
</script>
</body>
</html>
"""


@app.route("/")
def dashboard():
    return render_template_string(HTML)


if __name__ == "__main__":
    print(f"Dashboard: http://localhost:5050  (region={REGION}, table={TABLE})")
    app.run(host="0.0.0.0", port=5050, debug=True)
