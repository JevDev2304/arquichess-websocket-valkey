import asyncio, json, websockets, random
U='ws://127.0.0.1:8000/ws/'
async def rx(ws, pred, t=3):
    while True:
        m=json.loads(await asyncio.wait_for(ws.recv(), t))
        if pred(m): return m
R=[]
def ok(n,c): R.append((n,bool(c))); print(('PASS ' if c else 'FAIL ')+n)
async def main():
    room='e2e'+str(random.randint(10000,99999))
    a=await websockets.connect(U+room)
    await a.send(json.dumps({'type':'create','mode':'online','difficulty':'intern'}))
    seat=await rx(a,lambda m:m.get('type')=='seat'); ok('A recibe asiento blancas',seat['color']=='white')
    tokA=seat['token']
    st=await rx(a,lambda m:'fen' in m); ok('A recibe estado inicial',st['opponent_joined'] is False)
    b=await websockets.connect(U+room)
    await b.send(json.dumps({'type':'join'}))
    seatB=await rx(b,lambda m:m.get('type')=='seat'); ok('B recibe negras',seatB['color']=='black')
    tokB=seatB['token']
    upd=await rx(a,lambda m:m.get('opponent_joined') is True); ok('A es notificado por pub/sub de que B se unió',True)
    c=await websockets.connect(U+room); await c.send(json.dumps({'type':'join'}))
    e=await rx(c,lambda m:m.get('type')=='error'); ok('Tercer jugador rechazado (sala llena)',True); await c.close()
    d=await websockets.connect(U+room); await d.send(json.dumps({'type':'join','token':'falso'}))
    e=await rx(d,lambda m:m.get('type')=='error'); ok('Token inválido rechazado',True); await d.close()
    await b.send(json.dumps({'type':'move','move':'e7e5'}))
    e=await rx(b,lambda m:m.get('type')=='error'); ok('Negras no pueden mover en turno de blancas',True)
    await a.send(json.dumps({'type':'move','move':'e2e5'}))
    e=await rx(a,lambda m:m.get('type')=='error'); ok('Movimiento ilegal rechazado',True)
    await a.send(json.dumps({'type':'move','move':'e2e4'}))
    sa=await rx(a,lambda m:m.get('ply')==1); sb=await rx(b,lambda m:m.get('ply')==1)
    ok('Jugada legal llega a ambos clientes',sa['fen']==sb['fen'] and 'e4' in sa['last'])
    ok('Estado público no expone tokens/respuestas', 'seats' not in sa and 'answer' not in sa)
    qid=sa['question_queues']['white'][0] if 'question_queues' in sa else None
    ok('Pregunta de arquitectura encolada a blancas',qid is not None)
    await b.send(json.dumps({'type':'move','move':'e7e5'}))
    sb2=await rx(b,lambda m:m.get('ply')==2); ok('Negras mueven sin haber bloqueado por pregunta de blancas',True)
    await a.send(json.dumps({'type':'answer','question_id':qid,'answer':0}))
    fa=await rx(a,lambda m:(m.get('feedback_by') or {}).get('white'))
    ok('Respuesta produce feedback con explicación',fa['feedback_by']['white'].get('why'))
    # reconexión
    await a.close()
    a2=await websockets.connect(U+room); await a2.send(json.dumps({'type':'join','token':tokA}))
    s=await rx(a2,lambda m:m.get('type')=='seat'); ok('Reconexión de A con token recupera blancas',s['color']=='white')
    st=await rx(a2,lambda m:'fen' in m); ok('Estado recuperado desde Valkey tras reconectar',st['ply']==2)
    await b.send(json.dumps({'type':'move','move':'g1f3'}))  # no es turno de negras? ply2 -> turno blancas
    e=await rx(b,lambda m:m.get('type')=='error'); ok('Control de turno tras reconexión',True)
    await a2.send(json.dumps({'type':'move','move':'g1f3'}))
    await rx(b,lambda m:m.get('ply')==3); ok('Jugada tras reconexión llega a B',True)
    # sala inexistente
    z=await websockets.connect(U+'noexiste'+room); await z.send(json.dumps({'type':'join'}))
    e=await rx(z,lambda m:m.get('type')=='error'); ok('Unirse a sala inexistente da error',True)
    print('RESUMEN',sum(1 for _,c in R if c),'/',len(R))
asyncio.run(main())
