"""Demostración local: autoridad de partida, WebSocket y eventos Valkey."""
import asyncio
import json
import os
import random
import re
import secrets
from contextlib import asynccontextmanager
from pathlib import Path

import chess
import redis.asyncio as redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

URL = os.environ.get('VALKEY_URL', 'redis://localhost:6379/0')
from questions import QUESTIONS

LEVELS = ['Intern', 'Junior', 'Mid', 'Senior']

@asynccontextmanager
async def lifespan(app):
    app.state.db = redis.from_url(URL, decode_responses=True)
    await app.state.db.ping()
    yield
    await app.state.db.aclose()

app = FastAPI(lifespan=lifespan)

@app.get('/lessons.js')
async def lessons_js():
    return FileResponse(Path(__file__).parent / 'client' / 'lessons.js', media_type='application/javascript')

@app.get('/')
async def index():
    return FileResponse(Path(__file__).parent / 'client' / 'index.html')

def public(s):
    result = {k:v for k,v in s.items() if k not in ('answer', 'seats', 'question_order')}
    result['opponent_joined'] = bool(s.get('seats', {}).get('black')) if s.get('mode') == 'online' else True
    return result

async def announce(db, room, state):
    await db.set('chess:state:'+room, json.dumps(state))
    await db.publish('chess:event:'+room, json.dumps(public(state)))

async def forward(pubsub, ws):
    while True:
        msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
        if msg:
            await ws.send_text(msg['data'])
        else:
            await asyncio.sleep(.02)

def bot_move(board, level):
    moves = list(board.legal_moves)
    if not moves: return None
    if level == 0: return random.choice(moves)
    values = {chess.PAWN:1, chess.KNIGHT:3, chess.BISHOP:3, chess.ROOK:5, chess.QUEEN:9, chess.KING:0}
    def score(m):
        victim = board.piece_at(m.to_square)
        value = values.get(victim.piece_type,0) if victim else 0
        board.push(m)
        if board.is_checkmate(): value += 100
        elif board.is_check(): value += .5
        if level >= 2:
            for reply in board.legal_moves:
                target = board.piece_at(reply.to_square)
                if target: value -= values.get(target.piece_type,0) * (.15 if level == 2 else .5)
        board.pop()
        return value + random.random() * (1 if level == 1 else .1)
    return max(moves, key=score)

@app.websocket('/ws/{room}')
async def ws_game(ws: WebSocket, room: str):
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,40}',room):
        await ws.close(code=1008); return
    await ws.accept()
    db = app.state.db
    ps = db.pubsub()
    await ps.subscribe('chess:event:'+room)
    task = None
    key = 'chess:state:'+room
    player_token = None
    try:
        while True:
            msg = await ws.receive_json()
            async with db.lock('chess:lock:'+room, timeout=10, blocking_timeout=3):
                raw = await db.get(key)
                state = json.loads(raw) if raw else None
                if msg.get('type') == 'join':
                    if state is None:
                        await ws.send_json({'type':'error', 'message':'No existe una partida en esta sala.'})
                        continue
                    seats = state.get('seats')
                    if not seats:
                        await ws.send_json({'type':'error','message':'Esta partida antigua no tiene colores asignados. Crea una nueva.'})
                        continue
                    requested = msg.get('token')
                    if isinstance(requested, str) and requested and requested in seats.values():
                        player_token = requested
                    elif requested:
                        await ws.send_json({'type':'error','message':'La sesión guardada no corresponde a esta sala.'})
                        continue
                    elif state['mode'] == 'online' and not seats.get('black'):
                        player_token = secrets.token_urlsafe(24)
                        seats['black'] = player_token
                        state['last'] = 'Se unió el jugador de negras.'
                        await announce(db, room, state)
                    else:
                        await ws.send_json({'type':'error','message':'La sala está completa o la sesión guardada no corresponde a un jugador.'})
                        continue
                    color = next(c for c,t in seats.items() if t == player_token)
                    await ws.send_json({'type':'seat','color':color,'token':player_token})
                    await ws.send_json(public(state))
                    if task is None:
                        task = asyncio.create_task(forward(ps, ws))
                elif msg.get('type') == 'create' and state is not None:
                    await ws.send_json({'type':'error', 'message':'Ya existe una partida en esa sala. Únete con su código o crea una nueva.'})
                elif msg.get('type') == 'create' and state is None:
                    mode = msg.get('mode','online')
                    difficulty = msg.get('difficulty','easy')
                    if mode not in ('online','solo') or difficulty not in ('easy','medium','hard','intern','junior','mid','senior'): continue
                    player_token = secrets.token_urlsafe(24)
                    state = {'fen':chess.STARTING_FEN,'mode':mode,'difficulty':difficulty,'level':{'easy':0,'medium':2,'hard':3,'intern':0,'junior':1,'mid':2,'senior':3}[difficulty], 'pending':False,'count':0,'correct':0,'last':'Partida creada','question':None, 'feedback':None, 'question_order':random.sample(range(len(QUESTIONS)), len(QUESTIONS)), 'seats':{'white':player_token,'black':None}, 'pending_by':None, 'question_queues':{'white':[],'black':[]}, 'scores':{'white':{'correct':0,'answered':0},'black':{'correct':0,'answered':0}}, 'feedback_by':{'white':None,'black':None}, 'question_seq':0, 'last_move':None, 'ply':0}
                    await ws.send_json({'type':'seat','color':'white','token':player_token})
                    if task is None:
                        task = asyncio.create_task(forward(ps, ws))
                    await announce(db,room,state)
                elif state and msg.get('type') == 'move':
                    board=chess.Board(state['fen'])
                    if board.is_game_over():
                        await ws.send_json({'type':'error','message':'La partida de ajedrez ya terminó.'}); continue
                    color = 'white' if board.turn == chess.WHITE else 'black'
                    if not player_token or state.get('seats',{}).get(color) != player_token:
                        await ws.send_json({'type':'error','message':'No es tu turno o no tienes asignado ese color.'}); continue
                    if state['mode']=='online' and not state['seats'].get('black'):
                        await ws.send_json({'type':'error','message':'Espera a que se una el jugador de negras.'}); continue
                    try: move=chess.Move.from_uci(str(msg.get('move','')))
                    except ValueError: continue
                    if move not in board.legal_moves:
                        await ws.send_json({'type':'error','message':'Ese movimiento no es legal.'}); continue
                    san=board.san(move); board.push(move)
                    order = state.setdefault('question_order', random.sample(range(len(QUESTIONS)), len(QUESTIONS)))
                    queues = state.setdefault('question_queues', {'white':[],'black':[]})
                    seq = state.get('question_seq',0)
                    queues[color].append(order[seq % len(order)])
                    state['question_seq'] = seq + 1
                    state.setdefault('scores', {'white':{'correct':0,'answered':0},'black':{'correct':0,'answered':0}})
                    state.setdefault('feedback_by', {'white':None,'black':None})[color]=None
                    state['ply']=state.get('ply',0)+1
                    state['last_move']={'color':color,'san':san,'ply':state['ply']}
                    state.update(fen=board.fen(),last=('Blancas' if color=='white' else 'Negras')+' jugaron '+san,pending=False,question=None,pending_by=None)
                    if state['mode']=='solo' and not board.is_game_over():
                        move=bot_move(board,state['level'])
                        if move:
                            san=board.san(move); board.push(move); state['fen']=board.fen()
                            state['ply']+=1; state['last_move']={'color':'black','san':san,'ply':state['ply']}
                            state['last']='La IA jugó '+san
                    if board.is_game_over():
                        state['result']=board.result()
                    await announce(db,room,state)
                elif state and msg.get('type') == 'answer':
                    seats=state.get('seats',{})
                    color=next((c for c,t in seats.items() if t and t==player_token),None)
                    queues=state.get('question_queues',{})
                    if not color or not queues.get(color):
                        await ws.send_json({'type':'error','message':'No tienes una pregunta pendiente.'}); continue
                    question_id=queues[color][0]
                    if msg.get('question_id') != question_id:
                        await ws.send_json({'type':'error','message':'La pregunta cambió; espera a que se actualice.'}); continue
                    q=QUESTIONS[question_id]; answer=msg.get('answer')
                    if type(answer) is not int or not 0 <= answer < len(q['a']): continue
                    ok=answer==q['correct']; queues[color].pop(0)
                    scores=state.setdefault('scores',{'white':{'correct':0,'answered':0},'black':{'correct':0,'answered':0}})
                    scores[color]['answered']+=1; scores[color]['correct']+=int(ok)
                    if state['mode']=='solo' and not ok and state['difficulty'] not in ('hard','senior'):
                        state['level']=min(3,state['level']+1)
                    state.setdefault('feedback_by',{'white':None,'black':None})[color]={'ok':ok, 'why':q['why'], 'correct_answer':q['a'][q['correct']], 'question':q['q']}
                    await announce(db,room,state)
    except (WebSocketDisconnect, RuntimeError): pass
    finally:
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        await ps.aclose()

@app.get('/questions')
async def questions():
    return [{'q':q['q'],'a':q['a'],'topic':q['topic']} for q in QUESTIONS]

@app.post('/questions/{question_id}/answer')
async def answer_question(question_id: int, payload: dict):
    if not 0 <= question_id < len(QUESTIONS):
        from fastapi import HTTPException
        raise HTTPException(404, 'Pregunta no encontrada')
    q = QUESTIONS[question_id]
    answer = payload.get('answer')
    if type(answer) is not int or not 0 <= answer < len(q['a']):
        from fastapi import HTTPException
        raise HTTPException(400, 'Respuesta inválida')
    return {'ok': answer == q['correct'], 'why':q['why'], 'correct_answer':q['a'][q['correct']]} 

@app.get('/study/{topic}')
async def study_questions(topic: str):
    from fastapi import HTTPException
    from study_questions import STUDY_QUESTIONS
    if topic not in STUDY_QUESTIONS:
        raise HTTPException(404, 'Tema no encontrado')
    return [{'q':q['q'], 'a':q['a']} for q in STUDY_QUESTIONS[topic]]

@app.post('/study/{topic}/{question_id}/answer')
async def study_answer(topic: str, question_id: int, payload: dict):
    from fastapi import HTTPException
    from study_questions import STUDY_QUESTIONS
    questions = STUDY_QUESTIONS.get(topic)
    if questions is None or not 0 <= question_id < len(questions):
        raise HTTPException(404, 'Pregunta no encontrada')
    q = questions[question_id]
    answer = payload.get('answer')
    if type(answer) is not int or not 0 <= answer < len(q['a']):
        raise HTTPException(400, 'Respuesta inválida')
    return {'ok':answer == q['correct'], 'why':q['why'], 'correct_answer':q['a'][q['correct']]}
