# ArquiChess

Ajedrez multijugador didáctico con preguntas de arquitectura de software. Los navegadores se comunican por **WebSocket** con un backend **FastAPI**, y **Valkey** guarda el estado de cada sala y distribuye las actualizaciones por pub/sub.

Es una demostración local, no apta para producción.

## Cómo correrlo

Requisito: Docker (Docker Desktop, OrbStack o similar).

```bash
docker compose up --build
```

Abre http://localhost:8000.

`compose.yaml` levanta dos contenedores: la aplicación (construida con el `Dockerfile`) y Valkey. Valkey solo es accesible desde la red interna de Docker y la aplicación solo se publica en localhost.

Para detenerlo:

```bash
docker compose down
```

El volumen `partidas` conserva el estado entre reinicios. Para borrarlo también: `docker compose down -v`.

## Cómo jugar

1. «Jugar una partida» → «Dos jugadores» → «Crear partida nueva». Copia el código de sala.
2. En otra pestaña o ventana de incógnito, pega el código y pulsa «Unirme como segundo jugador».
3. Cada jugada trae una pregunta de arquitectura que puedes responder sin bloquear al rival.
4. «Recuperar mi sesión» permite volver a una partida tras una desconexión.

También hay modo individual contra una IA heurística (Intern, Junior, Mid, Senior) y un modo aprendizaje con clases y preguntas.

## Prueba de extremo a extremo (opcional)

Con la aplicación corriendo, en otra terminal:

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python e2e_dos_clientes.py
```
