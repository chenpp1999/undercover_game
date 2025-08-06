import eventlet

# Apply eventlet patch for async support. This MUST be done before importing threading.
eventlet.monkey_patch()

from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit, join_room, leave_room, disconnect
import random
import uuid
from collections import defaultdict
import os
import logging
from logging.handlers import RotatingFileHandler, QueueHandler, QueueListener
import threading
from datetime import datetime, timedelta
import socket
from queue import Queue  # Use standard library Queue for inter-thread communication
from pythonjsonlogger import jsonlogger


# ==============================================================================
# --- ADVANCED LOGGING SETUP (STRUCTURED, ASYNCHRONOUS, CONTEXTUAL) ---
# ==============================================================================
class ContextualLoggerAdapter(logging.LoggerAdapter):
    """
    A logger adapter to automatically inject contextual data into log records.
    """

    def process(self, msg, kwargs):
        if 'extra' in kwargs:
            kwargs['extra'] = {**self.extra, **kwargs['extra']}
        return msg, kwargs


def setup_logging():
    log_queue = Queue(-1)
    log_level = logging.DEBUG

    file_handler = RotatingFileHandler(
        'game_trace.log',
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding='utf-8'
    )
    formatter = jsonlogger.JsonFormatter(
        '%(asctime)s %(levelname)s %(name)s %(module)s %(funcName)s %(lineno)d '
        '%(message)s %(room_id)s %(player_id)s %(sid)s %(trace_id)s'
    )
    file_handler.setFormatter(formatter)

    listener = QueueListener(log_queue, file_handler, respect_handler_level=True)
    listener.start()

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    queue_handler = QueueHandler(log_queue)
    root_logger.addHandler(queue_handler)

    root_logger.info("=" * 60)
    root_logger.info("Logging system initialized: ASYNCHRONOUS and STRUCTURED (JSON).")
    root_logger.info(f"Log level set to {logging.getLevelName(log_level)}.")
    root_logger.info("=" * 60)

    return listener, root_logger


log_listener, root_logger = setup_logging()

# --- End of Logging Setup ---

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', os.urandom(24).hex())
socketio = SocketIO(app, logger=False, engineio_logger=False, async_mode='eventlet', cors_allowed_origins="*")

# --- Game State and Concurrency Management ---
games = {}
room_locks = defaultdict(lambda: eventlet.semaphore.Semaphore(1))
used_words = {}

WORD_FILE = "word\words.txt"
DEFAULT_WORDS = [
    ["加载失败", "加载失败"], ["加载失败", "加载失败"]
]

MAX_ROOM_IDLE_HOURS = 24
MAX_GAME_HISTORY_PER_ROOM = 10
CLEANUP_INTERVAL_MINUTES = 60


def find_player_id_by_sid(game_state, sid):
    for pid, player_data in game_state['players'].items():
        if player_data.get('sid') == sid:
            return pid
    return None


# --- ALL HELPER FUNCTIONS (UNCHANGED FROM USER'S ORIGINAL) ---
def load_word_pairs():
    global word_pairs
    try:
        if os.path.exists(WORD_FILE):
            with open(WORD_FILE, "r", encoding="utf-8") as f:
                word_pairs = [line.strip().split(',') for line in f if line.strip() and ',' in line.strip()]
        else:
            word_pairs = DEFAULT_WORDS
    except Exception:
        root_logger.error("Failed to load word file", exc_info=True)
        word_pairs = DEFAULT_WORDS


load_word_pairs()


def generate_room_id():
    return str(random.randint(100000, 999999))


def create_game_state():
    return {
        'players': {}, 'host_id': None, 'status': 'waiting', 'words': None,
        'undercover_players': [], 'eliminated_players': [], 'disconnected_players': {},
        'current_round': 1, 'speaking_order': [], 'speaking_direction': '顺时针',
        'current_speaker_index': 0, 'has_spoken': [],
        'descriptions': {}, 'votes': {}, 'tie_vote_candidates': [], 'is_tie_vote': False,
        'settings': {'undercover_count': 1, 'max_rounds': 10},
        'created_at': datetime.now(), 'game_history': [], 'total_games': 0, 'join_order': [],
        'last_game_result': None, 'last_activity': datetime.now()
    }


def assign_words_and_roles(game_state, room_id):
    players = list(game_state['players'].keys())
    if len(players) < 3: return False
    if room_id not in used_words: used_words[room_id] = []
    available_words = [pair for pair in word_pairs if pair not in used_words[room_id]]
    if not available_words:
        used_words[room_id] = []
        available_words = word_pairs
    word_pair = random.choice(available_words)
    used_words[room_id].append(word_pair)
    civilian_word, undercover_word = word_pair
    undercover_count = game_state['settings']['undercover_count']
    undercover_players = random.sample(players, undercover_count)
    for player_id in players:
        role = 'undercover' if player_id in undercover_players else 'civilian'
        word = undercover_word if role == 'undercover' else civilian_word
        game_state['players'][player_id].update({'word': word, 'role': role})
    game_state.update({
        'words': {'civilian': civilian_word, 'undercover': undercover_word},
        'undercover_players': undercover_players, 'eliminated_players': [], 'current_round': 1, 'status': 'speaking'
    })
    generate_speaking_order(game_state)
    return True


def generate_speaking_order(game_state):
    active_players = [p for p in game_state['players'].keys() if p not in game_state['eliminated_players']]
    random.shuffle(active_players)
    game_state['speaking_order'] = active_players
    game_state['speaking_direction'] = random.choice(['顺时针', '逆时针'])
    game_state['current_speaker_index'] = 0
    game_state['has_spoken'] = []


def check_game_end(game_state):
    active_player_ids = [pid for pid in game_state['players'].keys() if pid not in game_state['eliminated_players']]
    active_undercover = [p for p in active_player_ids if p in game_state['undercover_players']]
    active_civilians = len(active_player_ids) - len(active_undercover)
    if len(active_undercover) == 0: return 'civilians_win'
    if len(active_undercover) >= active_civilians: return 'undercover_win'
    if game_state['current_round'] > game_state['settings']['max_rounds']: return 'undercover_win'
    return None


def handle_player_reconnect(game_state, player_id, new_sid, logger):
    if player_id in game_state['disconnected_players']:
        player_info = game_state['disconnected_players'].pop(player_id)
        existing_player_with_sid = find_player_id_by_sid(game_state, new_sid)
        if existing_player_with_sid and existing_player_with_sid != player_id:
            logger.warning("Reconnect failed: New SID is already in use by another player.",
                           extra={'conflicting_player_id': existing_player_with_sid})
            return False
        player_info.update({'sid': new_sid, 'connected': True, 'reconnect_time': datetime.now()})
        game_state['players'][player_id] = player_info
        return True
    return False


def transfer_host(game_state, room_id):
    for player_id in game_state['join_order']:
        if player_id in game_state['players']:
            game_state['host_id'] = player_id
            return player_id
    for player_id in game_state['join_order']:
        if player_id in game_state['disconnected_players']:
            game_state['host_id'] = player_id
            return player_id
    return None


def reset_game_for_new_round(game_state):
    if game_state.get('words'):
        game_result = {
            'game_number': game_state['total_games'] + 1, 'words': game_state['words'],
            'players': [{'name': p['name'], 'role': p.get('role', 'unknown')} for p in
                        {**game_state['players'], **game_state['disconnected_players']}.values()],
            'finished_at': datetime.now().isoformat()
        }
        game_state['game_history'].append(game_result)
        if len(game_state['game_history']) > MAX_GAME_HISTORY_PER_ROOM:
            game_state['game_history'] = game_state['game_history'][-MAX_GAME_HISTORY_PER_ROOM:]
    game_state['total_games'] += 1
    game_state.update({
        'status': 'waiting', 'words': None, 'undercover_players': [], 'eliminated_players': [],
        'current_round': 1, 'speaking_order': [], 'current_speaker_index': 0, 'has_spoken': [],
        'descriptions': {}, 'votes': {}, 'tie_vote_candidates': [], 'is_tie_vote': False,
        'last_game_result': None, 'last_activity': datetime.now()
    })
    for p in {**game_state['players'], **game_state['disconnected_players']}.values():
        p.pop('word', None)
        p.pop('role', None)


def sync_game_state_for_player(room_id, player_id):
    game_state = games.get(room_id)
    if not game_state or (player_id not in game_state['players']): return
    logger = ContextualLoggerAdapter(root_logger, {'room_id': room_id, 'player_id': player_id})
    player_sid = game_state['players'][player_id]['sid']
    player_info = game_state['players'][player_id]
    all_players = {pid: {'name': p['name']} for pid, p in game_state['players'].items()}
    disconnected_players = {pid: {'name': p['name']} for pid, p in game_state['disconnected_players'].items()}
    state_payload = {
        'status': game_state['status'], 'host_id': game_state['host_id'],
        'my_word': player_info.get('word'), 'my_role': player_info.get('role'),
        'round': game_state['current_round'], 'game_number': game_state['total_games'],
        'eliminated_players': game_state['eliminated_players'], 'all_players': all_players,
        'disconnected_players': disconnected_players, 'settings': game_state['settings'],
        'last_game_result': game_state.get('last_game_result')
    }
    if game_state['status'] == 'speaking':
        state_payload['speaking_order'] = [
            {'id': pid, 'name': all_players.get(pid, disconnected_players.get(pid, {})).get('name', '未知')} for pid in
            game_state['speaking_order'] if pid in all_players or pid in disconnected_players]
        state_payload['speaking_direction'] = game_state.get('speaking_direction', '顺时针')
    elif game_state['status'] == 'voting':
        candidates = game_state.get('tie_vote_candidates', [])
        if not candidates: candidates = [p for p in {**all_players, **disconnected_players}.keys() if
                                         p not in game_state['eliminated_players']]
        state_payload['voting_candidates'] = [
            {'id': pid, 'name': all_players.get(pid, disconnected_players.get(pid, {})).get('name', '未知')} for pid in
            candidates if pid in all_players or pid in disconnected_players]
        state_payload['is_tie_vote'] = game_state['is_tie_vote']
    emit('sync_state', state_payload, room=player_sid)
    logger.debug("SYNC_STATE sent to player", extra={'sid': player_sid, 'payload': state_payload})


def cleanup_inactive_rooms():
    logger = ContextualLoggerAdapter(root_logger, {'trace_id': 'cleanup_task'})
    logger.info("Starting inactive room cleanup task...")
    inactive_rooms = []
    for room_id, game_state in list(games.items()):
        idle_duration = datetime.now() - game_state.get('last_activity', game_state['created_at'])
        if idle_duration > timedelta(hours=MAX_ROOM_IDLE_HOURS):
            inactive_rooms.append(room_id)
    if not inactive_rooms:
        logger.info("Cleanup task finished: No inactive rooms found.")
        return
    for room_id in inactive_rooms:
        logger.warning("Room is inactive and will be removed.", extra={'room_id': room_id})
        lock = room_locks.get(room_id)
        if lock and lock.acquire(blocking=False):
            try:
                games.pop(room_id, None)
                used_words.pop(room_id, None)
                room_locks.pop(room_id, None)
                logger.info("Successfully removed inactive room.", extra={'room_id': room_id})
            finally:
                lock.release()
        else:
            logger.error("Could not acquire lock for inactive room, will retry next cycle.", extra={'room_id': room_id})
    logger.info(f"Cleanup task finished. Removed {len(inactive_rooms)} rooms.")


def update_room_activity(room_id):
    if room_id in games:
        games[room_id]['last_activity'] = datetime.now()


def start_cleanup_scheduler():
    def cleanup_task():
        while True:
            try:
                cleanup_inactive_rooms()
            except Exception:
                root_logger.error("An exception occurred during cleanup task", exc_info=True)
            eventlet.sleep(CLEANUP_INTERVAL_MINUTES * 60)

    eventlet.spawn(cleanup_task)


@app.route('/')
def index(): return render_template('index.html')


@app.route('/game/<room_id>')
def game_room(room_id): return render_template('game.html', room_id=room_id)


# --- SocketIO Event Handlers ---

@socketio.on('connect')
def on_connect():
    sid = request.sid
    logger = ContextualLoggerAdapter(root_logger, {'sid': sid, 'ip': request.remote_addr})
    logger.info("Client connected")


@socketio.on('ping')
def on_ping():
    emit('pong')


@socketio.on('disconnect')
def on_disconnect():
    sid = request.sid
    logger = ContextualLoggerAdapter(root_logger, {'sid': sid})
    room_id_to_process, player_id_to_disconnect = None, None
    for r_id, g_state in list(games.items()):
        p_id = find_player_id_by_sid(g_state, sid)
        if p_id:
            room_id_to_process, player_id_to_disconnect = r_id, p_id
            break
    if not room_id_to_process:
        logger.info("Disconnected SID not found in any active game room.")
        return
    logger = ContextualLoggerAdapter(logger, {'room_id': room_id_to_process, 'player_id': player_id_to_disconnect})
    logger.info("Client disconnected from a room.")
    with room_locks[room_id_to_process]:
        game_state = games.get(room_id_to_process)
        if not game_state or player_id_to_disconnect not in game_state['players']:
            logger.warning("Disconnected player no longer in active list, likely already handled.")
            return
        player_info = game_state['players'].pop(player_id_to_disconnect)
        player_info.update({'connected': False, 'disconnect_time': datetime.now()})
        game_state['disconnected_players'][player_id_to_disconnect] = player_info
        update_room_activity(room_id_to_process)
        logger.info("Player moved to disconnected list.", extra={'player_name': player_info.get('name')})
        original_host_id = game_state['host_id']
        new_host_id = None
        if player_id_to_disconnect == original_host_id:
            logger.info("Host disconnected. Starting host transfer process.")
            new_host_id = transfer_host(game_state, room_id_to_process)
            if new_host_id:
                all_known_players = {**game_state['players'], **game_state['disconnected_players']}
                new_host_player = all_known_players.get(new_host_id)
                emit('host_transferred',
                     {'new_host_id': new_host_id, 'new_host_name': new_host_player.get('name', '未知'),
                      'reason': 'original_host_disconnected'}, room=room_id_to_process)
                logger.info("Host transferred to new player.",
                            extra={'new_host_id': new_host_id, 'new_host_name': new_host_player.get('name')})
            else:
                logger.warning("Host transfer failed. No eligible player found.")
        if game_state['status'] == 'finished' and new_host_id:
            logger.info("Re-emitting game_finished event after host transfer on results page.")
            result_payload = game_state.get('last_game_result')
            if result_payload:
                result_payload['host_id'] = game_state['host_id']
                emit('game_finished', result_payload, room=room_id_to_process)
        emit('player_list_update', {'players': {pid: {'name': p['name']} for pid, p in game_state['players'].items()},
                                    'disconnected_players': {pid: {'name': p['name']} for pid, p in
                                                             game_state['disconnected_players'].items()},
                                    'host_id': game_state['host_id']}, room=room_id_to_process)


# --- THE REMAINDER OF THE CODE IS NOW COMPLETE AND LOGGING-ENHANCED ---

@socketio.on('create_room')
def on_create_room(data):
    sid = request.sid
    player_name = data.get('player_name', '').strip()
    logger = ContextualLoggerAdapter(root_logger,
                                     {'sid': sid, 'player_name': player_name, 'trace_id': str(uuid.uuid4())[:8]})
    logger.info("Received create_room request.")
    if not 1 <= len(player_name) <= 10:
        logger.error("Create room failed: Invalid nickname length.")
        return emit('error', {'message': '昵称需为1-10个字符'})
    room_id = generate_room_id()
    while room_id in games:
        room_id = generate_room_id()
    logger = ContextualLoggerAdapter(logger, {'room_id': room_id})
    with room_locks[room_id]:
        player_id = str(uuid.uuid4())
        logger = ContextualLoggerAdapter(logger, {'player_id': player_id})
        games[room_id] = create_game_state()
        games[room_id]['host_id'] = player_id
        games[room_id]['players'][player_id] = {'name': player_name, 'sid': sid, 'connected': True,
                                                'join_time': datetime.now()}
        games[room_id]['join_order'].append(player_id)
        join_room(room_id)
    logger.info("Room created successfully.")
    emit('room_created', {'room_id': room_id, 'player_id': player_id, 'player_name': player_name})


@socketio.on('join_room')
def on_join_room(data):
    trace_id = str(uuid.uuid4())[:8]
    sid = request.sid
    room_id, player_name, player_id = data.get('room_id'), data.get('player_name', '').strip(), data.get('player_id')
    logger = ContextualLoggerAdapter(root_logger,
                                     {'sid': sid, 'ip': request.remote_addr, 'trace_id': trace_id, 'room_id': room_id,
                                      'player_name': player_name, 'player_id': player_id})
    logger.info("Received join_room request.")
    if not room_id or not player_name:
        logger.error("Join room failed: Missing room_id or player_name.")
        return emit('error', {'message': '房间号和玩家名不能为空'})
    if room_id not in games:
        logger.error("Join room failed: Room does not exist.")
        return emit('error', {'message': '房间不存在'})
    with room_locks[room_id]:
        game_state = games[room_id]
        if game_state['status'] == 'finished':
            logger.info("Player joining a finished game. Resetting room to waiting state.", extra={'player_name': player_name})
            reset_game_for_new_round(game_state)
            # Notify everyone in the room that the state has been reset
            emit('player_list_update',
                 {'players': {pid: {'name': p['name']} for pid, p in game_state['players'].items()},
                  'disconnected_players': {pid: {'name': p['name']} for pid, p in game_state['disconnected_players'].items()},
                  'host_id': game_state['host_id']},
                 room=room_id)
        update_room_activity(room_id)
        logger.debug("State before join attempt.", extra={'active_players': list(game_state['players'].keys()),
                                                          'disconnected_players': list(
                                                              game_state['disconnected_players'].keys())})
        player_id_with_same_name = next(
            (pid for pid, p_data in game_state['players'].items() if p_data['name'] == player_name), None)
        if player_id_with_same_name:
            old_sid = game_state['players'][player_id_with_same_name].get('sid')
            logger.warning("Implicit session takeover: Player with same name is active. Updating SID.",
                           extra={'player_id': player_id_with_same_name, 'old_sid': old_sid})
            game_state['players'][player_id_with_same_name]['sid'] = sid
            if old_sid and old_sid != sid:
                disconnect(old_sid, silent=True)
                logger.info("Disconnected old SID.", extra={'old_sid': old_sid})
            join_room(room_id)
            emit('joined_successfully',
                 {'room_id': room_id, 'player_id': player_id_with_same_name, 'player_name': player_name,
                  'host_id': game_state['host_id'], 'game_in_progress': game_state['status'] != 'waiting'})
            sync_game_state_for_player(room_id, player_id_with_same_name)
            emit('player_list_update',
                 {'players': {pid: {'name': p['name']} for pid, p in game_state['players'].items()},
                  'disconnected_players': {pid: {'name': p['name']} for pid, p in
                                           game_state['disconnected_players'].items()},
                  'host_id': game_state['host_id']}, room=room_id)
            return
        reconnecting_player_id = None
        if player_id and player_id in game_state['disconnected_players']:
            reconnecting_player_id = player_id
        else:
            reconnecting_player_id = next(
                (pid for pid, p_data in game_state['disconnected_players'].items() if p_data['name'] == player_name),
                None)
        if reconnecting_player_id:
            logger = ContextualLoggerAdapter(logger, {'player_id': reconnecting_player_id})
            logger.info("Attempting to reconnect player.")
            if handle_player_reconnect(game_state, reconnecting_player_id, sid, logger):
                logger.info("Player reconnected successfully.")
                join_room(room_id)
                emit('joined_successfully', {'room_id': room_id, 'player_id': reconnecting_player_id,
                                             'player_name': game_state['players'][reconnecting_player_id]['name'],
                                             'host_id': game_state['host_id'],
                                             'game_in_progress': game_state['status'] != 'waiting'})
                sync_game_state_for_player(room_id, reconnecting_player_id)
                emit('player_list_update',
                     {'players': {pid: {'name': p['name']} for pid, p in game_state['players'].items()},
                      'disconnected_players': {pid: {'name': p['name']} for pid, p in
                                               game_state['disconnected_players'].items()},
                      'host_id': game_state['host_id']}, room=room_id)
                return
        logger.info("No existing session found. Joining as a new player.")
        if game_state['status'] != 'waiting':
            logger.error("Join failed: Game already in progress.")
            return emit('error', {'message': '游戏已开始，无法加入'})
        if (len(game_state['players']) + len(game_state['disconnected_players'])) >= 10:
            logger.error("Join failed: Room is full.")
            return emit('error', {'message': '房间已满'})
        new_player_id = str(uuid.uuid4())
        logger = ContextualLoggerAdapter(logger, {'player_id': new_player_id})
        game_state['players'][new_player_id] = {'name': player_name, 'sid': sid, 'connected': True,
                                                'join_time': datetime.now()}
        game_state['join_order'].append(new_player_id)
        join_room(room_id)
        logger.info("New player joined successfully.")
        emit('joined_successfully', {'room_id': room_id, 'player_id': new_player_id, 'player_name': player_name,
                                     'host_id': game_state['host_id'], 'game_in_progress': False})
        emit('player_list_update', {'players': {pid: {'name': p['name']} for pid, p in game_state['players'].items()},
                                    'disconnected_players': {pid: {'name': p['name']} for pid, p in
                                                             game_state['disconnected_players'].items()},
                                    'host_id': game_state['host_id']}, room=room_id)


@socketio.on('start_game')
@socketio.on('continue_game')
def on_start_game(data):
    room_id = data.get('room_id')
    with room_locks[room_id]:
        game_state = games.get(room_id)
        if not game_state: return
        player_id = find_player_id_by_sid(game_state, request.sid)
        logger = ContextualLoggerAdapter(root_logger, {'room_id': room_id, 'player_id': player_id, 'sid': request.sid,
                                                       'event': request.event['message']})
        if not player_id or game_state['host_id'] != player_id:
            logger.error("Start/Continue game failed: Not the host.")
            return emit('error', {'message': '只有房主可以开始游戏'})
        if len(game_state['players']) < 3:
            logger.error("Start/Continue game failed: Not enough players.",
                         extra={'player_count': len(game_state['players'])})
            return emit('error', {'message': '至少需要3名在线玩家才能开始'})
        update_room_activity(room_id)
        total_players = len(game_state['players'])
        max_allowed_undercovers = max(1, (total_players - 1) // 2)
        if game_state['settings']['undercover_count'] > max_allowed_undercovers:
            game_state['settings']['undercover_count'] = max_allowed_undercovers
            logger.info("Undercover count adjusted to fit player count.", extra={'new_count': max_allowed_undercovers})
        reset_game_for_new_round(game_state)
        if not assign_words_and_roles(game_state, room_id):
            logger.error("Failed to assign words and roles.")
            return emit('error', {'message': '分配词语失败'})
        logger.info("Game started/continued successfully.",
                    extra={'game_number': game_state['total_games'], 'words': game_state['words']})
        all_known_players = {**game_state['players'], **game_state['disconnected_players']}
        for pid, player in game_state['players'].items():
            emit('game_started', {'word': player['word'], 'round': 1, 'game_number': game_state['total_games']},
                 room=player['sid'])
        emit('speaking_phase', {'host_id': game_state['host_id'],
                                'speaking_order': [{'id': pid, 'name': all_known_players[pid]['name']} for pid in
                                                   game_state['speaking_order'] if pid in all_known_players],
                                'speaking_direction': game_state.get('speaking_direction', '顺时针'), 'round': 1,
                                'all_players': {pid: {'name': p['name']} for pid, p in all_known_players.items()},
                                'eliminated_players': []}, room=room_id)
        emit('all_speakers_finished', room=room_id)


@socketio.on('end_game')
def on_end_game(data):
    room_id = data.get('room_id')
    with room_locks[room_id]:
        game_state = games.get(room_id)
        player_id = find_player_id_by_sid(game_state, request.sid)
        logger = ContextualLoggerAdapter(root_logger, {'room_id': room_id, 'player_id': player_id, 'sid': request.sid})
        if not game_state:
            logger.error("End game failed: Room not found.")
            return emit('error', {'message': '房间不存在'})
        if not player_id or game_state['host_id'] != player_id:
            logger.error("End game failed: Not the host.")
            return emit('error', {'message': '只有房主可以结束游戏'})
        if game_state['status'] == 'waiting':
            logger.warning("End game failed: Game not started yet.")
            return emit('error', {'message': '游戏尚未开始'})
        update_room_activity(room_id)
        game_state['status'] = 'finished'
        all_players_with_roles = [{'name': p['name'], 'role': p.get('role', 'civilian'), 'id': pid} for pid, p in
                                  {**game_state['players'], **game_state['disconnected_players']}.items()]
        result_payload = {'result': 'game_ended_by_host',
                          'words': game_state.get('words', {'civilian': '未知', 'undercover': '未知'}),
                          'all_players': all_players_with_roles, 'eliminated_player': None, 'can_continue': True,
                          'host_id': game_state['host_id'], 'forced_end': True}
        game_state['last_game_result'] = result_payload
        emit('game_finished', result_payload, room=room_id)
        logger.info("Host forced game to end.")


# --- START: FIXED AND COMPLETED VOTING LOGIC ---

@socketio.on('force_start_vote')
def on_force_start_vote(data):
    room_id = data.get('room_id')
    with room_locks[room_id]:
        game_state = games.get(room_id)
        if not game_state: return
        player_id = find_player_id_by_sid(game_state, request.sid)
        logger = ContextualLoggerAdapter(root_logger, {'room_id': room_id, 'player_id': player_id, 'sid': request.sid})
        if not player_id or game_state['host_id'] != player_id:
            logger.error("Force start vote failed: Not the host.")
            return emit('error', {'message': '只有房主可以开始投票'})
        update_room_activity(room_id)
        game_state['status'] = 'voting'
        game_state['votes'] = {}
        game_state['tie_vote_candidates'] = []
        game_state['is_tie_vote'] = False
        all_players = {**game_state['players'], **game_state['disconnected_players']}
        candidates = [{'id': pid, 'name': p['name']} for pid, p in all_players.items() if
                      pid not in game_state['eliminated_players']]
        all_players_info = {pid: {'name': p['name']} for pid, p in all_players.items()}
        emit('voting_phase', {'all_players': all_players_info, 'voting_candidates': candidates,
                              'eliminated_players': game_state['eliminated_players'], 'is_tie_vote': False},
             room=room_id)
        logger.info("Voting phase started.", extra={'round': game_state['current_round']})


@socketio.on('submit_vote')
def on_submit_vote(data):
    room_id, voted_player_id = data.get('room_id'), data.get('voted_player_id')
    with room_locks[room_id]:
        game_state = games.get(room_id)
        if not game_state: return
        voter_id = find_player_id_by_sid(game_state, request.sid)
        logger = ContextualLoggerAdapter(root_logger, {'room_id': room_id, 'player_id': voter_id, 'sid': request.sid})
        if not voter_id or game_state['status'] != 'voting':
            logger.error("Submit vote failed: Not in voting phase.")
            return emit('error', {'message': '当前不在投票阶段'})
        if voter_id in game_state['eliminated_players']:
            logger.error("Submit vote failed: Eliminated player cannot vote.")
            return emit('error', {'message': '已淘汰的玩家不能投票'})
        if voter_id in game_state['votes']:
            logger.warning("Submit vote failed: Player already voted.")
            return emit('error', {'message': '你已经投过票了'})
        update_room_activity(room_id)
        game_state['votes'][voter_id] = voted_player_id
        logger.info("Player submitted a vote.", extra={'voted_for': voted_player_id})
        active_voters = [pid for pid in game_state['players'] if pid not in game_state['eliminated_players']]
        emit('vote_status_update',
             {'voted_players': list(game_state['votes'].keys()), 'total_voters': len(active_voters),
              'current_votes': len(game_state['votes'])}, room=room_id)
        if len(game_state['votes']) >= len(active_voters):
            logger.info("All active players have voted. Handling results.")
            handle_vote_result(room_id)


def handle_vote_result(room_id):
    game_state = games[room_id]
    logger = ContextualLoggerAdapter(root_logger, {'room_id': room_id})
    vote_counts = defaultdict(int)
    for voter, voted_for in game_state['votes'].items():
        vote_counts[voted_for] += 1
    logger.info("Calculating vote results.", extra={'vote_counts': dict(vote_counts)})
    emit('vote_results_update', {'votes': dict(vote_counts)}, room=room_id)
    socketio.sleep(3)
    if not vote_counts:
        logger.warning("No votes were cast. Starting next round.")
        emit('tie_vote', {'tied_players': []}, room=room_id)
        start_next_round(room_id, None)
        return
    max_votes = max(vote_counts.values())
    eliminated_candidates = [pid for pid, count in vote_counts.items() if count == max_votes]
    if len(eliminated_candidates) > 1 and not game_state['is_tie_vote']:
        logger.info("Tie vote occurred. Starting re-vote.", extra={'tied_players': eliminated_candidates})
        game_state.update(
            {'is_tie_vote': True, 'tie_vote_candidates': eliminated_candidates, 'votes': {}, 'status': 'voting'})
        all_players = {**game_state['players'], **game_state['disconnected_players']}
        emit('tie_vote',
             {'tied_players': [{'id': pid, 'name': all_players[pid]['name']} for pid in eliminated_candidates]},
             room=room_id)
        all_players_info = {pid: {'name': p['name']} for pid, p in all_players.items()}
        emit('voting_phase', {'all_players': all_players_info,
                              'voting_candidates': [{'id': pid, 'name': all_players[pid]['name']} for pid in
                                                    eliminated_candidates],
                              'eliminated_players': game_state['eliminated_players'], 'is_tie_vote': True},
             room=room_id)
        return
    eliminated_player_id = eliminated_candidates[0]
    game_state['eliminated_players'].append(eliminated_player_id)
    game_state['is_tie_vote'] = False
    game_state['tie_vote_candidates'] = []
    all_players = {**game_state['players'], **game_state['disconnected_players']}
    eliminated_player_info = {'id': eliminated_player_id,
                              'name': all_players.get(eliminated_player_id, {}).get('name', '未知')}
    logger.info("Player has been eliminated.", extra={'eliminated_player': eliminated_player_info})
    start_next_round(room_id, eliminated_player_info)


def start_next_round(room_id, just_eliminated_player):
    game_state = games[room_id]
    logger = ContextualLoggerAdapter(root_logger, {'room_id': room_id})
    game_status = check_game_end(game_state)
    if game_status:
        game_state['status'] = 'finished'
        all_players_with_roles = [{'name': p['name'], 'role': p.get('role', 'civilian'), 'id': pid} for pid, p in
                                  {**game_state['players'], **game_state['disconnected_players']}.items()]
        result_payload = {'result': game_status, 'words': game_state['words'], 'all_players': all_players_with_roles,
                          'eliminated_player': just_eliminated_player, 'can_continue': True,
                          'host_id': game_state['host_id']}
        game_state['last_game_result'] = result_payload
        emit('game_finished', result_payload, room=room_id)
        logger.info("Game finished.", extra={'result': game_status})
    else:
        game_state.update(
            {'current_round': game_state['current_round'] + 1, 'status': 'speaking', 'votes': {}, 'is_tie_vote': False,
             'tie_vote_candidates': []})
        generate_speaking_order(game_state)
        all_players = {**game_state['players'], **game_state['disconnected_players']}
        emit('speaking_phase', {'host_id': game_state['host_id'],
                                'speaking_order': [{'id': pid, 'name': all_players[pid]['name']} for pid in
                                                   game_state['speaking_order'] if pid in all_players],
                                'speaking_direction': game_state.get('speaking_direction', '顺时针'),
                                'round': game_state['current_round'],
                                'all_players': {pid: {'name': p['name']} for pid, p in all_players.items()},
                                'eliminated_players': game_state['eliminated_players'],
                                'just_eliminated': just_eliminated_player}, room=room_id)
        emit('all_speakers_finished', room=room_id)
        logger.info("Starting next round.", extra={'round': game_state['current_round']})


# --- END: FIXED AND COMPLETED VOTING LOGIC ---

@socketio.on('kick_player')
def on_kick_player(data):
    room_id, player_id_to_kick = data.get('room_id'), data.get('player_id_to_kick')
    if not room_id or not player_id_to_kick: return
    with room_locks[room_id]:
        game_state = games.get(room_id)
        if not game_state: return
        kicker_id = find_player_id_by_sid(game_state, request.sid)
        logger = ContextualLoggerAdapter(root_logger, {'room_id': room_id, 'player_id': kicker_id, 'sid': request.sid})
        if not kicker_id or kicker_id != game_state['host_id']:
            return logger.error("Kick player failed: Not the host.",
                                extra={'kicked_player_id': player_id_to_kick}) or emit('error',
                                                                                       {'message': '只有房主才能踢人'})
        if game_state['status'] != 'waiting':
            return logger.error("Kick player failed: Game in progress.",
                                extra={'kicked_player_id': player_id_to_kick}) or emit('error', {
                'message': '游戏开始后无法踢人'})
        if kicker_id == player_id_to_kick:
            return logger.error("Kick player failed: Host cannot kick self.") or emit('error',
                                                                                      {'message': '不能踢自己'})
        update_room_activity(room_id)
        kicked_player_info = game_state['players'].pop(player_id_to_kick, None) or game_state[
            'disconnected_players'].pop(player_id_to_kick, None)
        if kicked_player_info:
            logger.info("Player kicked successfully.", extra={'kicked_player_id': player_id_to_kick,
                                                              'kicked_player_name': kicked_player_info.get('name')})
            game_state['join_order'] = [pid for pid in game_state['join_order'] if pid != player_id_to_kick]
            kicked_sid = kicked_player_info.get('sid')
            if kicked_player_info.get('connected') and kicked_sid:
                emit('you_were_kicked', {'message': '您已被房主移出房间'}, room=kicked_sid)
                disconnect(kicked_sid, silent=True)
            emit('player_list_update',
                 {'players': {pid: {'name': p['name']} for pid, p in game_state['players'].items()},
                  'disconnected_players': {pid: {'name': p['name']} for pid, p in
                                           game_state['disconnected_players'].items()},
                  'host_id': game_state['host_id']}, room=room_id)


@socketio.on('update_settings')
def on_update_settings(data):
    room_id, undercover_count = data.get('room_id'), data.get('undercover_count')
    with room_locks[room_id]:
        game_state = games.get(room_id)
        if not game_state: return
        player_id = find_player_id_by_sid(game_state, request.sid)
        logger = ContextualLoggerAdapter(root_logger, {'room_id': room_id, 'player_id': player_id, 'sid': request.sid})
        if not player_id or game_state['host_id'] != player_id:
            logger.error("Update settings failed: Not the host.")
            return emit('error', {'message': '只有房主可以修改设置'})
        update_room_activity(room_id)
        try:
            count = int(undercover_count)
            total_players = len(game_state['players']) + len(game_state['disconnected_players'])
            max_allowed = max(1, (total_players - 1) // 2)
            game_state['settings']['undercover_count'] = max(1, min(count, max_allowed))
            logger.info("Settings updated.", extra={'settings': game_state['settings']})
        except (ValueError, TypeError):
            logger.error("Update settings failed: Invalid value for undercover_count.",
                         extra={'value': undercover_count})
        emit('settings_updated', {'undercover_count': game_state['settings']['undercover_count']}, room=room_id)


@socketio.on('get_room_history')
def on_get_room_history(data):
    room_id = data.get('room_id')
    game_state = games.get(room_id)
    if not game_state: return
    logger = ContextualLoggerAdapter(root_logger, {'room_id': room_id})
    logger.info("Player requested room history.")
    update_room_activity(room_id)
    emit('room_history', {'history': game_state['game_history']})


def get_lan_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


if __name__ == '__main__':
    load_word_pairs()
    start_cleanup_scheduler()
    port = int(os.environ.get('PORT', 5000))
    host = '0.0.0.0'
    local_ip = get_lan_ip()
    print("=" * 50)
    print("🎮 谁是卧底游戏服务器启动 (v3.1 完整修复版)")
    print("✅ 后端并发锁已优化为 eventlet non-blocking。")
    print("✅ 内存自动清理机制已启动。")
    print("✅ 日志系统已升级为异步、结构化(JSON)模式。")
    print("✅ 已修复投票阶段无法进行的问题。")
    print("🔴 详细日志将被记录到 game_trace.log 文件中。")
    print(f"💻 本地访问: http://127.0.0.1:{port}")
    print(f"🌐 局域网访问 (如在同一网络下): http://{local_ip}:{port}")
    print("=" * 50)
    try:
        socketio.run(app, host=host, port=port, log_output=False, use_reloader=False)
    finally:
        root_logger.info("Server shutting down. Stopping log listener.")
        log_listener.stop()
